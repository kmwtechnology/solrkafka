package com.kmwllc.solr.solrkafka.test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kmwllc.solr.solrkafka.datatype.solr.SolrDocumentSerializer;
import com.kmwllc.solr.solrkafka.importer.KafkaImporter;
import com.kmwllc.solr.solrkafka.test.docproducer.TestDocumentCreator;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.common.SolrDocument;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * Runs tests in cloud mode. Kills a node while indexing then brings it back up. Sets up the nodes automatically.
 * Should be run on the node on port 8983.
 */
public class MultiNodeKillTest implements AutoCloseable {
  private static final Logger log = LogManager.getLogger(MultiNodeKillTest.class);
  private static final ObjectMapper mapper = new ObjectMapper();
  private static final String topic = "testtopic";
  private static final String collectionName = "cloudKillTest";
  private final static String solrHostPath = "http://localhost:8983/solr/";
  private final String kafkaHostPath;
  private static final String kafkaPort = ":9092";
  private final TestDocumentCreator docs;
  private final boolean ignoreShardRouting;
  private static final int NUM_DOCS = 15_000;
  private final SolrManager manager;
  private volatile long numDocsSeeded = 0;
  private volatile boolean seedDocs = true;
  private final File solrBinDir;

  /**
   * @param docsPath The path to a JSON file of solr documents to test with, or null if docs should be randomly created
   * @param docker Whether or not this test is being run in docker (uses different URLs for services)
   * @param ignoreShardRouting Whether or not shard routing should be ignored in this test
   * @param solrBinDir The "bin" directory containing the Solr CLI manager executable
   */
  public MultiNodeKillTest(Path docsPath, boolean docker, boolean ignoreShardRouting, Path solrBinDir) throws IOException {
    this.solrBinDir = solrBinDir == null ? null : solrBinDir.toFile();
    manager = new SolrManager(collectionName, mapper);
    log.info("Loading test documents");
    if (docsPath != null) {
      docs = new TestDocumentCreator(mapper.readValue(docsPath.toFile(), new TypeReference<List<SolrDocument>>() {}));
    } else {
      docs = new TestDocumentCreator(NUM_DOCS, 10_000);
    }

    this.kafkaHostPath = docker ? "kafka" : "localhost";
    this.ignoreShardRouting = ignoreShardRouting;
  }

  public static void main(String[] args) throws Exception {
    Path docsPath = null;
    boolean docker = false;
    Path configPath = null;
    Path solrBinDir = null;
    boolean ignoreShardRouting = false;

    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-d")) {
        // Required if this is being run in docker
        docker = true;
      } else if (args[i].equals("-p") && args.length > i + 1) {
        // The path to a Solr doc JSON
        docsPath = Path.of(args[++i]);
      } else if (args[i].equals("-c") && args.length > i + 1) {
        // The path to a configuration file
        configPath = Path.of(args[++i]);
      } else if (args[i].equals("-i")) {
        // This test should perform all shard routing
        ignoreShardRouting = true;
      } else if (args[i].equals("--solr-bin-dir") && args.length > i + 1) {
        // The directory that the Solr CLI manager executable is in (ex ~/Documents/solr/bin/)
        solrBinDir = Path.of(args[++i]);
      } else {
        log.fatal("Unknown param passed, usage: [-d] [-p DOCS_PATH] [-c CONFIG_PATH] [--cname COLLECTION_NAME] " +
            "[-i] [--solr-bin-dir SOLR_BIN_DIR]");
        System.exit(1);
      }
    }

    if (configPath == null) {
      configPath = ignoreShardRouting
          ? Path.of("/opt/solr/server/solr/configsets/testconfig/conf/solrconfig-routing.xml")
          : Path.of("/opt/solr/server/solr/configsets/testconfig/conf/solrconfig.xml");
    }

    try (MultiNodeKillTest test = new MultiNodeKillTest(docsPath, docker, ignoreShardRouting, solrBinDir)) {
      test.manager.uploadConfigAndCreateCollection("2", "2", "0", "0", configPath);
      test.manager.forceCommit();
      MultiNodeTest.checkDocCount(0, test.manager, collectionName, ignoreShardRouting);
      test.manager.manageImporter(true);
      test.runTest();
    } catch (Throwable e) {
      log.error("Exception occurred while running test, exiting with status code 1", e);
      System.exit(1);
    }
    log.info("Test successfully completed");
  }

  @Override
  public void close() throws IOException {
    manager.close();
  }

  /**
   * A 'runnable' method to seed Kafka at a rate of 1 doc per second
   */
  private void seedDocs() {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaHostPath + kafkaPort);
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "MultiNodeTestProducer");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, SolrDocumentSerializer.class.getName());

    try (Producer<String, SolrDocument> producer = new KafkaProducer<>(props)) {
      while (docs.hasNext() && seedDocs) {
        SolrDocument doc = docs.next();
        ProducerRecord<String, SolrDocument> record = new ProducerRecord<>(topic, doc.get("id").toString(), doc);
        producer.send(record);
        // Fine that this is non-atomic, we're not accessing it outside of thread until thread is dead
        if (++numDocsSeeded % 5 == 0) {
          log.info("Added {} docs to Kafka", numDocsSeeded);
        }
        // 1 doc/sec
        Thread.sleep(1000);
      }
    } catch (InterruptedException e) {
      log.error("Seeder thread interrupted, exiting", e);
    }
    log.info("Seeded {} docs", numDocsSeeded);
  }

  /**
   * Run the test by confirming doc count is 0, seeding Kafka (if applicable), waiting for the importer to catch up,
   * and checking the doc count.
   */
  public void runTest() throws IOException, InterruptedException, ExecutionException, TimeoutException {
    final long start = System.currentTimeMillis();

    // Start seeding documents into Kafka in a separate thread
    log.info("Sending {} documents to topic {}", docs.size(), topic);
    CompletableFuture<Void> docSupplier = CompletableFuture.runAsync(this::seedDocs);

    // Let seeding happen normally for a couple of seconds
    log.info("Sleeping for 10 seconds to let docs be seeded");
    Thread.sleep(10000);

    // Stop Solr and wait for it to come back online
    CompletableFuture<Boolean> solrManagerThread = manager.stopSolr(solrBinDir)
        .thenComposeAsync(val -> manager.waitForSolrOnline());
    log.info("Waiting up to 5 minutes for a thread to finish");

    // Wait up to 5 minutes for either thread to finish
    CompletableFuture.anyOf(docSupplier, solrManagerThread).get(5, TimeUnit.MINUTES);
    log.info("A thread finished, waiting 10 seconds to continue test");

    // Let importing happen normally for 10 seconds after all nodes are online
    Thread.sleep(10000);

    // Stop the seeder and wait up to 15 seconds for the thread to exit
    seedDocs = false;
    docSupplier.get(15, TimeUnit.SECONDS);
    long numDocs = KafkaImporter.getEndOffsets(kafkaHostPath + kafkaPort, topic).values().stream()
        .mapToLong(l -> l).sum();
    log.info("Number of docs seeded is {}, Expected number of docs seeded is {}", numDocs, numDocsSeeded);
    if (numDocs != numDocsSeeded) {
      throw new IllegalStateException("Num docs is not equal to num docs seeded, test assumption failed (this says nothing about the test though...)");
    }

    // Check that the solrManagerThread successfully completed
    if (!solrManagerThread.isDone() || solrManagerThread.isCompletedExceptionally() || !solrManagerThread.get()) {
      throw new IllegalStateException("Waited for test setup to finish but manager thread still running " +
          "or completed exceptionally");
    }

    // Continue test normally (see MultiNodeTest)
    manager.waitForLag(numDocsSeeded);
    final double duration = (System.currentTimeMillis() - start) / 1000.0;
    manager.forceCommit();
    MultiNodeTest.checkDocCount(numDocsSeeded, manager, collectionName, ignoreShardRouting);
    log.info("Duration for {} docs was {} seconds, {} docs / second", numDocsSeeded, duration,
        numDocsSeeded / duration);
    log.info("Test passed");
  }

}
