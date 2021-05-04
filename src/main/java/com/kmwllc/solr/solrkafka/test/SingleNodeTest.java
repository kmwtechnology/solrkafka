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
import java.util.List;
import java.util.Properties;

/**
 * Runs tests outside of cloud mode. Expects the node to be set up prior to running the test.
 */
public class SingleNodeTest implements AutoCloseable {
  private static final Logger log = LogManager.getLogger(SingleNodeTest.class);
  private static final ObjectMapper mapper = new ObjectMapper();
  private final SolrManager manager;
  private static final String topic = "testtopic";
  private final static String solrHostPath = "http://localhost:8983/solr/";
  private final String kafkaHostPath;
  private static final String solrPath = "singleNodeTest";
  private static final String pluginEndpoint = "/kafka";
  private static final String kafkaPort = ":9092";
  private final TestDocumentCreator docs;
  private static final int NUM_DOCS = 15_000;
  private static final int DOC_SIZE = 10_000;

  /**
   * @param docsPath The path to a JSON file of solr documents to test with, or null if docs should be randomly created
   * @param docker Whether or not this test is being run in docker (uses different URLs for services)
   */
  public SingleNodeTest(Path docsPath, boolean docker) throws IOException {
    manager = new SolrManager(solrPath, mapper);

    log.info("Loading test documents");
    if (docsPath != null) {
      docs = new TestDocumentCreator(mapper.readValue(docsPath.toFile(), new TypeReference<List<SolrDocument>>() {}));
    } else {
      docs = new TestDocumentCreator(NUM_DOCS, DOC_SIZE);
    }

    this.kafkaHostPath = docker ? "kafka" : "localhost";
  }

  public static void main(String[] args) throws IOException {
    if (!List.of(0, 1, 3, 4).contains(args.length)) {
      throw new IllegalStateException("Invalid number of args. Must either be of the form " +
          "\"[-p DOCS_PATH] [-d]\"");
    }

    Path docsPath = null;
    boolean docker = false;
    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-d")) {
        docker = true;
      } else if (args[i].equals("-p") && args.length > i + 1) {
        docsPath = Path.of(args[++i]);
      }
    }

    try (SingleNodeTest test = new SingleNodeTest(docsPath, docker)) {
      test.manager.manageImporter(true);
      test.runTest();
    } catch (Throwable e) {
      log.error("Error occurred while running test", e);
    }
  }

  @Override
  public void close() throws IOException {
    manager.close();
  }

  /**
   * Checks the number of documents found on the node to ensure it matches the expected count.
   *
   * @param numRecords The expected number of records
   */
  private void checkDocCount(int numRecords) throws IOException {
    HttpGet get = new HttpGet(solrHostPath + solrPath + "/select?q=*:*&rows=0");
    String bodyString = manager.makeRequest(get);
    JsonNode body = mapper.readTree(bodyString);

    if (!body.has("response") || !body.get("response").has("numFound")) {
      throw new IllegalStateException("Invalid response from Solr");
    }
    if (body.get("response").get("numFound").longValue() != numRecords) {
      String msg = String.format("Incorrect number of documents found; expected %d but found %d",
          numRecords, body.get("response").get("numFound").longValue());
      throw new IllegalStateException(msg);
    }
    log.info("Doc count is as expected");
  }

  /**
   * Run the test by confirming doc count is 0, seeding Kafka (if applicable), waiting for the importer to catch up,
   * and checking the doc count.
   */
  public void runTest() throws IOException {
    manager.forceCommit();
    checkDocCount(0);

    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaHostPath + kafkaPort);
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "SingleNodeTestProducer");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, SolrDocumentSerializer.class.getName());

    log.info("Sending {} documents to topic {}", docs.size(), topic);
    final long start = System.currentTimeMillis();
    try (Producer<String, SolrDocument> producer = new KafkaProducer<>(props)) {
      for (SolrDocument doc : docs) {
        ProducerRecord<String, SolrDocument> record = new ProducerRecord<>(topic, doc.get("id").toString(), doc);
        producer.send(record);
      }
    }
    log.info("Done sending documents, waiting until consumer lag is 0");
    waitForLag(docs.size());
    final double duration = (System.currentTimeMillis() - start) / 1000.0;
    manager.forceCommit();
    checkDocCount(docs.size());

    log.info("Duration for {} docs was {} seconds, {} docs / second", docs.size(), duration,
        docs.size() / duration);
  }

  /**
   * Waits for the consumer group lag to be 0 for each Kafka partition or the importer to stop.
   * If it's not reached in 45 seconds, an execption is thrown.
   */
  public void waitForLag(int numDocs) throws IOException {
    HttpGet get = new HttpGet(solrHostPath + solrPath + pluginEndpoint + "?action=status");
    int round = 0;
    int numStatic = 0;
    while (true) {
      try {
        if (numStatic > 3) {
          throw new IllegalStateException("Waited " + numStatic +
              " rounds, but documents could not all be consumed from Kafka");
        }
        log.info("Sleeping for 5 seconds on round {}", round++);
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        log.info("Interrupted while sleeping");
        return;
      }

      String bodyString = manager.makeRequest(get);
      JsonNode body = mapper.readTree(bodyString);

      if (body.has("status") && body.get("status").textValue().equals("STOPPED")) {
        log.info("Status is stopped, checking core state");
        return;
      }
      if (body.has("consumer_group_lag") && body.get("consumer_group_lag").isObject()) {
        JsonNode lag = body.get("consumer_group_lag");
        boolean finished = true;
        if (lag.size() == 0) {
          continue;
        }
        int offsetSums = 0;
        for (JsonNode partition : lag) {
          if (partition.asLong() > 0) {
            offsetSums += partition.asLong();
            finished = false;
          }
        }
        if (!finished) {
          if (offsetSums == numDocs) {
            numStatic++;
          }
          continue;
        }
        return;
      }
    }
  }
}
