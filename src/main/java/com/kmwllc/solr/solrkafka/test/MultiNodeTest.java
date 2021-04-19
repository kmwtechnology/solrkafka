package com.kmwllc.solr.solrkafka.test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kmwllc.solr.solrkafka.datatype.solr.SolrDocumentSerializer;
import org.apache.commons.io.FileUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.utils.URIBuilder;
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
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.zookeeper.KeeperException;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class MultiNodeTest {
  private static final Logger log = LogManager.getLogger(MultiNodeTest.class);
  private static final ObjectMapper mapper = new ObjectMapper();
  private final CloseableHttpClient client = HttpClients.createDefault();
  private static final String topic = "testtopic";
  private final static String solrHostPath = "http://localhost:8983/solr/";
  private final String kafkaHostPath;
  private static final String collectionName = "cloudTest";
  private static final String pluginEndpoint = "/kafka";
  private static final String kafkaPort = ":9092";
  private final List<SolrDocument> docs;
  private final Path configPath;
  private String leader;
  private final boolean ignoreShardRouting;

  static {
    ConsoleAppender appender = new ConsoleAppender();
    org.apache.log4j.Logger.getRootLogger().addAppender(appender);
  }

  public MultiNodeTest(Path docsPath, boolean docker, Path configPath, boolean ignoreShardRouting) throws IOException {
    log.info("Loading test documents");
    if (docsPath != null) {
      docs = mapper.readValue(docsPath.toFile(), new TypeReference<List<SolrDocument>>() {});
    } else {
      docs = new TestDocumentCreator(256).createDocs();
    }

    this.kafkaHostPath = docker ? "kafka" : "localhost";

    this.configPath = configPath;

    this.ignoreShardRouting = ignoreShardRouting;
  }

  public static void main(String[] args) throws Exception {
    Path docsPath = null;
    boolean docker = false;
    Path configPath = Path.of("/opt/solr/server/solr/configsets/testconfig/conf/solrconfig.xml");
    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-d")) {
        docker = true;
      } else if (args[i].equals("-p") && args.length > i + 1) {
        docsPath = Path.of(args[++i]);
      } else if (args[i].equals("-c") && args.length > i + 1) {
        configPath = Path.of(args[++i]);
      }
    }

    MultiNodeTest test = new MultiNodeTest(docsPath, docker, configPath, false);
    test.uploadConfig();
    test.manageImporter(true);
    try {
      test.runTest();
    } finally {
      test.manageImporter(false);
    }
  }

  public void uploadConfig() throws IOException, KeeperException, URISyntaxException {
    String zkAddr = System.getenv("ZK_HOST");
    Path temp = null;
    try (SolrZkClient client = new SolrZkClient(zkAddr == null ? "localhost:2181" : zkAddr, 30000)) {
      if (!client.exists("/configs/solrkafka", false)) {
        temp = Files.createTempDirectory("solrkafka-test");
        client.downConfig("_default", temp);
        client.upConfig(temp, "solrkafka");
        byte[] data = Files.readAllBytes(configPath);
        client.setData("/configs/solrkafka/solrconfig.xml", data, true);
      }
    } catch (InterruptedException e) {
      throw new IOException(e);
    } finally {
      if (temp != null) {
        FileUtils.forceDelete(temp.toFile());
      }
    }

    URIBuilder bldr = new URIBuilder(solrHostPath + "admin/collections")
        .setParameter("action", "CREATE").setParameter("numShards", "2")
        .setParameter("router.name", "compositeId").addParameter("replicationFactor", "2")
        .addParameter("maxShardsPerNode", "-1").addParameter("name", collectionName)
        .addParameter("collection.configName", "solrkafka");
    makeRequest(new HttpGet(bldr.build()), 200, 400);
  }

  public String makeRequest(HttpUriRequest req) throws IOException {
    return makeRequest(req, 200);
  }

  public String makeRequest(HttpUriRequest req, Integer... expectedStatuses) throws IOException {
    log.info("Sending request to: {}", req.getURI());
    try (CloseableHttpResponse res = client.execute(req);
         BufferedInputStream entity = new BufferedInputStream(res.getEntity().getContent())) {
      if (!Arrays.asList(expectedStatuses).contains(res.getStatusLine().getStatusCode())) {
        log.error("Invalid response received: {}", new String(entity.readAllBytes()));
        throw new IllegalStateException("Unexpected status code received");
      }
      String body = new String(entity.readAllBytes());
      log.info("Received response: {}", body);
      return body;
    }
  }

  public void manageImporter(boolean start) throws IOException {
    if (leader == null) {
      HttpGet get = new HttpGet(solrHostPath + "admin/cores?action=STATUS");
      String bodyString = makeRequest(get);
      JsonNode body = mapper.readTree(bodyString);

      String nodeName = null;
      for (JsonNode node : body.get("status")) {
        nodeName = node.get("name").textValue();
        log.info("{} SolrKafka importer", start ? "starting" : "stopping");
        get = new HttpGet(solrHostPath + nodeName + pluginEndpoint + (start ? "" : "?action=stop"));
        bodyString = makeRequest(get);
        body = mapper.readTree(bodyString);
        if (body.get("leader").booleanValue()) {
          break;
        }
      }
      leader = nodeName;
    } else {
      log.info("{} SolrKafka importer", start ? "starting" : "stopping");
      HttpGet get = new HttpGet(solrHostPath + leader + pluginEndpoint + (start ? "" : "?action=stop"));
      makeRequest(get);
    }
  }

  public void forceCommit() throws IOException {
    log.info("Forcing commit");
    HttpGet get = new HttpGet(solrHostPath + leader + "/update?commit=true");
    makeRequest(get);
  }

  private void checkDocCount(int numRecords) throws IOException {
    HttpGet get = new HttpGet(solrHostPath + leader + "/select?q=*:*&rows=0");
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
    log.info("Total doc count is as expected");

    get = new HttpGet(solrHostPath + "admin/cores?action=STATUS");
    bodyString = makeRequest(get);
    body = mapper.readTree(bodyString);
    if (!body.has("status") || !body.get("status").isObject() || body.get("status").size() != 4) {
      throw new IllegalStateException("Body doesn't contain status or node info");
    }

    List<String> errors = new ArrayList<>();
    Map<String, Long> docCounter = new HashMap<>();
    Map<String, Integer> versions = new HashMap<>();

    for (JsonNode node : body.get("status")) {
      if (!node.isObject() || !node.has("index") ||
          !node.get("index").has("numDocs") || !node.get("index").has("version")) {
        errors.add("Node does not contain numDocs or version fields: " + node.get("name"));
      }

      String shard = node.get("cloud").get("shard").textValue();

      if (docCounter.containsKey(shard)) {
        if (node.get("index").get("numDocs").longValue() != docCounter.get(shard)) {
          errors.add("Node does not have " + docCounter.get(shard) + " records: " + node.get("name") + " = "
              + node.get("index").get("numDocs").longValue());
        }
      } else {
        docCounter.put(shard, node.get("index").get("numDocs").longValue());
      }

      if (versions.containsKey(shard)) {
        if (node.get("index").get("version").intValue() != versions.get(shard)) {
          errors.add("Node does not have " + versions.get(shard) + " version number: " + node.get("name") + " = "
              + node.get("index").get("version").intValue());
        }
      } else {
        versions.put(shard, node.get("index").get("version").intValue());
      }
    }

    if (ignoreShardRouting) {
      for (Map.Entry<String, Long> entry : docCounter.entrySet()) {
        if (entry.getValue() != numRecords) {
          errors.add("Shard doesn't have " + numRecords + " records: " + entry.getKey() + " = " + entry.getValue());
        }
      }
      int version = -1;
      for (Map.Entry<String, Integer> entry : versions.entrySet()) {
        if (version == -1) {
          version = entry.getValue();
        } else if (entry.getValue() != version) {
          errors.add("Shard doesn't have version " + version + ": " + entry.getKey() + " = " + entry.getValue());
        }
      }
    } else {
      long sum = docCounter.values().stream().mapToLong(v -> v).sum();
      if (sum != numRecords) {
        errors.add("Shard doc counts don't add up to " + numRecords + ": " + sum);
      }
    }

    if (errors.size() != 0) {
      throw new IllegalStateException("Nodes had incorrect values: \n" + errors.stream()
          .collect(StringBuilder::new, (sb, s) -> sb.append(s).append("\n"), (sb1, sb2) -> sb1.append(sb2).append("\n")));
    }
    log.info("Expected doc and version numbers for shards found");
  }

  public void runTest() throws IOException {
    forceCommit();
    checkDocCount(0);

    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaHostPath + kafkaPort);
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "MultiNodeTestProducer");
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
    HttpGet get = new HttpGet(solrHostPath + leader + pluginEndpoint + "?action=status");
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
