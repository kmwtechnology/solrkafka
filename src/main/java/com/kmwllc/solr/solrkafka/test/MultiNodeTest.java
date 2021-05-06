package com.kmwllc.solr.solrkafka.test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kmwllc.solr.solrkafka.datatype.solr.SolrDocumentSerializer;
import com.kmwllc.solr.solrkafka.test.docproducer.TestDocumentCreator;
import org.apache.http.client.methods.HttpGet;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.common.SolrDocument;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Runs tests in cloud mode. Sets up the nodes automatically.
 */
public class MultiNodeTest implements AutoCloseable {
  private static final Logger log = LogManager.getLogger(MultiNodeTest.class);
  private static final ObjectMapper mapper = new ObjectMapper();
  private static final String topic = "testtopic";
  private final static String solrHostPath = "http://localhost:8983/solr/";
  private final String kafkaHostPath;
  private final String collectionName;
  private static final String kafkaPort = ":9092";
  private final TestDocumentCreator docs;
  private final boolean ignoreShardRouting;
  private final boolean skipSeedKafka;
  private static final int NUM_DOCS = 15_000;
  private static final int DOC_SIZE = 2500;
  private final SolrManager manager;

  /**
   * @param collectionName The name of the collection to (try to) create and test
   * @param docsPath The path to a JSON file of solr documents to test with, or null if docs should be randomly created
   * @param docker Whether or not this test is being run in docker (uses different URLs for services)
   * @param ignoreShardRouting Whether or not shard routing should be ignored in this test
   * @param skipSeedKafka Should be {@code true} if this test has already been run once on an active cluster (avoids re-seeding Kafka)
   */
  public MultiNodeTest(String collectionName, Path docsPath, boolean docker, boolean ignoreShardRouting,
                       boolean skipSeedKafka) throws IOException {
    manager = new SolrManager(collectionName, mapper);
    log.info("Loading test documents");
    if (docsPath != null) {
      docs = new TestDocumentCreator(mapper.readValue(docsPath.toFile(), new TypeReference<List<SolrDocument>>() {}));
    } else {
      docs = new TestDocumentCreator(NUM_DOCS, DOC_SIZE);
    }

    this.collectionName = collectionName;
    this.kafkaHostPath = docker ? "kafka" : "localhost";
    this.ignoreShardRouting = ignoreShardRouting;
    this.skipSeedKafka = skipSeedKafka;
  }

  public static void main(String[] args) throws Exception {
    Path docsPath = null;
    boolean docker = false;
    boolean skipSeedKafka = false;
    String collectionName = "cloudTest";
    String pulls = "0";
    String tlogs = "0";
    String nrts = "2";
    String shards = "2";
    Path configPath = null;
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
      } else if (args[i].equals("--cname") && args.length > i + 1) {
        // The collection name
        collectionName = args[++i];
      } else if (args[i].equals("--pulls") && args.length > i + 1) {
        // The number of PULL replicas
        pulls = args[++i];
      } else if (args[i].equals("--tlogs") && args.length > i + 1) {
        // The number ot TLOG replicas
        tlogs = args[++i];
      } else if ((args[i].equals("--nrts") || args[i].equals("-r")) && args.length > i + 1) {
        // The number of NRT replicas
        nrts = args[++i];
      } else if (args[i].equals("-s") && args.length > i + 1) {
        // The number of shards
        shards = args[++i];
      } else if (args[i].equals("-k")) {
        // If provided, kafka will not be seeded with randomized docs or anything provided with -p
        skipSeedKafka = true;
      } else if (args[i].equals("-i")) {
        // Tests all shard routing functionality
        ignoreShardRouting = true;
      } else {
        log.fatal("Unknown param passed {}, usage: [-d] [-p DOCS_PATH] [-c CONFIG_PATH] [--cname COLLECTION_NAME] [-k]" +
            "[--pulls NUM_PULL_REPLICAS] [--tlogs NUM_TLOG_REPLICAS] [--nrts|-r NUM_NRT_REPLICAS] [-s NUM_SHARDS] [-i] [--kill-node]",
            args[i]);
        System.exit(1);
      }
    }

    if (configPath == null) {
      configPath = ignoreShardRouting
          ? Path.of("/opt/solr/server/solr/configsets/testconfig/conf/solrconfig-routing.xml")
          : Path.of("/opt/solr/server/solr/configsets/testconfig/conf/solrconfig.xml");
    }

    try (MultiNodeTest test = new MultiNodeTest(collectionName, docsPath, docker, ignoreShardRouting,
        skipSeedKafka)) {
      test.manager.uploadConfigAndCreateCollection(shards, nrts, pulls, tlogs, configPath);
      test.manager.forceCommit();
      MultiNodeTest.checkDocCount(0, test.manager, collectionName, ignoreShardRouting);
      test.manager.manageImporter(true);
      test.runTest();
    } catch (Throwable e) {
      log.error("Exception occurred while setting up/checking initial state", e);
    }
  }

  @Override
  public void close() throws IOException {
    manager.close();
  }

  /**
   * Check if the doc count in each of the replicas is as expected and make sure all nodes are still active.
   *
   * @param numRecords The number of records that are expected to be found
   */
  static void checkDocCount(int numRecords, SolrManager manager, String collectionName, boolean ignoreShardRouting)
      throws IOException {
    if (numRecords > 0) {
      log.info("Sleeping for 5 seconds to let commit complete");
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        throw new IllegalStateException("Interrupted while waiting", e);
      }
    }

    HttpGet get = new HttpGet(solrHostPath + "admin/collections?action=CLUSTERSTATUS&collection=" + collectionName);
    String bodyString = manager.makeRequest(get);
    JsonNode body = mapper.readTree(bodyString);

    List<String> errors = new ArrayList<>();

    // Determine if all of the cores are active
    for (JsonNode shard : body.get("cluster").get("collections").get(collectionName).get("shards")) {
      for (JsonNode replica : shard.get("replicas")) {
        if (!replica.get("state").textValue().equals("active")) {
          errors.add(replica.get("core").textValue());
        }
      }
    }
    if (!errors.isEmpty()) {
      throw new IllegalStateException("Nodes not active " + errors);
    }

    // Try to check the number of documents each node contains up to 25 times
    for (int i = 0; i < 25; i++) {
      errors.clear();
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        throw new IllegalStateException("Interrupted while waiting", e);
      }

      // Select all docs to determine if the correct number of documents overall are found if not ignoring shard routing
      if (!ignoreShardRouting) {
        get = new HttpGet(solrHostPath + collectionName + "/select?q=*:*&rows=0");
        bodyString = manager.makeRequest(get);
        body = mapper.readTree(bodyString);

        if (!body.has("response") || !body.get("response").has("numFound")) {
          throw new IllegalStateException("Invalid response from Solr");
        }
        if (body.get("response").get("numFound").longValue() != numRecords) {
          String msg = String.format("Incorrect number of documents found; expected %d but found %d",
              numRecords, body.get("response").get("numFound").longValue());
          errors.add(msg);
        } else {
          log.info("Total doc count is as expected");
        }
      }

      // Get the status of each core to determine if it contains the correct number of documents
      get = new HttpGet(solrHostPath + "admin/cores?action=STATUS");
      bodyString = manager.makeRequest(get);
      body = mapper.readTree(bodyString);

      if (!body.has("status") || !body.get("status").isObject() || body.get("status").size() == 0) {
        throw new IllegalStateException("Body doesn't contain status or node info");
      }

      // Each replica within a shard should have the same number of docs
      // Each shard's doc count should add up to the expected number of docs, unless ignoreShardRouting is true
      //   In that case, each shard's doc count should be the expected number of docs
      Map<String, Long> docCounter = new HashMap<>();

      for (JsonNode node : body.get("status")) {
        // Skip if this core is in the wrong collection
        if (!node.get("cloud").get("collection").textValue().equals(collectionName)) {
          continue;
        }

        if (!node.isObject() || !node.has("index") ||
            !node.get("index").has("numDocs") || !node.get("index").has("version")) {
          errors.add("Node does not contain numDocs or version fields: " + node.get("name"));
        }

        String shard = node.get("cloud").get("shard").textValue();

        if (docCounter.containsKey(shard)) {
          // If this core's shard has already been found, make sure the core's doc count is the same
          if (node.get("index").get("numDocs").longValue() != docCounter.get(shard)) {
            errors.add("Node does not have " + docCounter.get(shard) + " records: " + node.get("name") + " = "
                + node.get("index").get("numDocs").longValue());
          }
        } else {
          docCounter.put(shard, node.get("index").get("numDocs").longValue());
        }
      }

      if (ignoreShardRouting) {
        // Make sure each shard has the expected number of records
        for (Map.Entry<String, Long> entry : docCounter.entrySet()) {
          if (entry.getValue() != numRecords) {
            errors.add("Shard doesn't have " + numRecords + " records: " + entry.getKey() + " = " + entry.getValue());
          }
        }
      } else {
        // Make sure the sum of each shard's records is the expected number of records
        long sum = docCounter.values().stream().mapToLong(v -> v).sum();
        if (sum != numRecords) {
          errors.add("Shard doc counts don't add up to " + numRecords + ": " + sum);
        }
      }

      // Log the errors, but don't throw until enough rounds have passed
      if (!errors.isEmpty()) {
        log.info("Nodes had incorrect values: \n{}", errors.stream()
            .collect(StringBuilder::new, (sb, s) -> sb.append(s).append("\n"),
                (sb1, sb2) -> sb1.append(sb2).append("\n")).toString());
      } else {
        // Return if the test passed
        log.info("Expected doc and version numbers for shards found");
        return;
      }
      if (i % 5 == 0) {
        log.info("Sleeping for 5 seconds on round {} of state check", i);
        manager.forceCommit();
      }
    }
    throw new IllegalStateException("Errors found in state after 25 rounds of requests");
  }

  /**
   * Run the test by confirming doc count is 0, seeding Kafka (if applicable), waiting for the importer to catch up,
   * and checking the doc count.
   */
  public void runTest() throws IOException {

    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaHostPath + kafkaPort);
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "MultiNodeTestProducer");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, SolrDocumentSerializer.class.getName());

    final long start = System.currentTimeMillis();
    if (!skipSeedKafka) {
      log.info("Sending {} documents to topic {}", docs.size(), topic);
      int i = 0;
      try (Producer<String, SolrDocument> producer = new KafkaProducer<>(props)) {
        for (SolrDocument doc : docs) {
          ProducerRecord<String, SolrDocument> record = new ProducerRecord<>(topic, doc.get("id").toString(), doc);
          producer.send(record);
          if (++i % 1000 == 0) {
            log.info("Added {} docs to Kafka", i);
          }
        }
      }
      log.info("Done sending documents, waiting until consumer lag is 0");
    } else {
      log.info("Skip seed Kafka");
    }

    manager.waitForLag(docs.size());
    final double duration = (System.currentTimeMillis() - start) / 1000.0;
    manager.forceCommit();
    checkDocCount(docs.size(), manager, collectionName, ignoreShardRouting);
    log.info("Duration for {} docs was {} seconds, {} docs / second", docs.size(), duration,
        docs.size() / duration);
    log.info("Test passed");
  }

}
