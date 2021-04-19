package com.kmwllc.solr.solrkafka.test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kmwllc.solr.solrkafka.datatype.solr.SolrDocumentSerializer;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.ConsoleAppender;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.common.SolrDocument;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;

public class SingleNodeTest {
  private static final Logger log = LogManager.getLogger(SingleNodeTest.class);
  private static final ObjectMapper mapper = new ObjectMapper();
  private final CloseableHttpClient client = HttpClients.createDefault();
  private static final String topic = "testtopic";
  private final static String solrHostPath = "http://localhost";
  private final String kafkaHostPath;
  private static final String solrPath = ":8983/solr/singleNodeTest";
  private static final String pluginEndpoint = "/kafka";
  private static final String kafkaPort = ":9092";
  private final List<SolrDocument> docs;

  static {
    ConsoleAppender appender = new ConsoleAppender();
    org.apache.log4j.Logger.getRootLogger().addAppender(appender);
  }

  public SingleNodeTest(Path docsPath, boolean docker) throws IOException {
    log.info("Loading test documents");
    if (docsPath != null) {
      docs = mapper.readValue(docsPath.toFile(), new TypeReference<List<SolrDocument>>() {});
    } else {
      docs = new TestDocumentCreator(256).createDocs();
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

    SingleNodeTest test = new SingleNodeTest(docsPath, docker);
    test.manageImporter(true);
    try {
      test.runTest();
    } finally {
      test.manageImporter(false);
    }
  }

  public String makeRequest(HttpUriRequest req) throws IOException {
    try (CloseableHttpResponse res = client.execute(req);
         BufferedInputStream entity = new BufferedInputStream(res.getEntity().getContent())) {
      if (res.getStatusLine().getStatusCode() < 200 || res.getStatusLine().getStatusCode() >= 300) {
        throw new IllegalStateException("Non-200 status code received");
      }
      return new String(entity.readAllBytes());
    }
  }

  public void manageImporter(boolean start) throws IOException {
    log.info("{} SolrKafka importer", start ? "starting" : "stopping");
    HttpGet get = new HttpGet(solrHostPath + solrPath + pluginEndpoint + (start ? "" : "?action=stop"));
    String bodyString = makeRequest(get);
    log.info("Response received: {}", bodyString);
  }

  public void forceCommit() throws IOException {
    log.info("Forcing commit");
    HttpGet get = new HttpGet(solrHostPath + solrPath + "/update?commit=true");
    makeRequest(get);
  }

  private void checkDocCount(int numRecords) throws IOException {
    HttpGet get = new HttpGet(solrHostPath + solrPath + "/select?q=*:*&rows=0");
    String bodyString = makeRequest(get);
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

  public void runTest() throws IOException {
    forceCommit();
    checkDocCount(0);

    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaHostPath + kafkaPort);
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "SingleNodeTestProducer");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, SolrDocumentSerializer.class.getName());

    log.info("Sending {} documents to topic {}", docs.size(), topic);
    try (Producer<String, SolrDocument> producer = new KafkaProducer<>(props)) {
      for (SolrDocument doc : docs) {
        ProducerRecord<String, SolrDocument> record = new ProducerRecord<>(topic, doc.get("id").toString(), doc);
        producer.send(record);
      }
    }
    log.info("Done sending documents, waiting until consumer lag is 0");
    waitForLag();
    forceCommit();
    checkDocCount(docs.size());
  }

  public void waitForLag() throws IOException {
    HttpGet get = new HttpGet(solrHostPath + solrPath + pluginEndpoint + "?action=status");
    final int maxWait = 45;
    int waitCount = 0;
    while (true) {
      try {
        if (waitCount > maxWait) {
          throw new IllegalStateException("Waited for " + maxWait +
              " seconds, but documents could not all be consumed from Kafka");
        }
        Thread.sleep(1000);
        waitCount++;
      } catch (InterruptedException e) {
        log.info("Interrupted while sleeping");
        return;
      }

      String bodyString = makeRequest(get);
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
        for (JsonNode partition : lag) {
          if (partition.asLong() > 0) {
            finished = false;
            break;
          }
        }
        if (!finished) {
          continue;
        }
        return;
      }
    }
  }

}
