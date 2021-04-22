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

/**
 * Runs tests in cloud mode. Sets up the nodes automatically.
 */
public class MultiNodeTest {
  private static final Logger log = LogManager.getLogger(MultiNodeTest.class);
  private static final ObjectMapper mapper = new ObjectMapper();
  private final CloseableHttpClient client = HttpClients.createDefault();
  private static final String topic = "testtopic";
  private final static String solrHostPath = "http://localhost:8983/solr/";
  private final String kafkaHostPath;
  private final String collectionName;
  private static final String pluginEndpoint = "/kafka";
  private static final String kafkaPort = ":9092";
  private final TestDocumentCreator docs;
  private final Path configPath;
  private String leader;
  private final boolean ignoreShardRouting;
  private final String tlogs;
  private final String pulls;
  private final String nrts;
  private final String shards;
  private final boolean skipSeedKafka;
  private static final int NUM_DOCS = 15_000;

  /**
   * @param collectionName The name of the collection to (try to) create and test
   * @param docsPath The path to a JSON file of solr documents to test with, or null if docs should be randomly created
   * @param docker Whether or not this test is being run in docker (uses different URLs for services)
   * @param configPath The path to the solrconfig.xml file to use in the collection within the Solr container,
   *                   or null if a premade solrconfig.xml should be used
   * @param ignoreShardRouting Whether or not shard routing should be ignored in this test
   * @param nrts The number of NRT replicas to create
   * @param tlogs The numer of TLOG replicas to create
   * @param pulls The number of PULL replicas to create
   * @param shards The number of shards to create
   * @param skipSeedKafka Should be {@code true} if this test has already been run once on an active cluster (avoids re-seeding Kafka)
   */
  public MultiNodeTest(String collectionName, Path docsPath, boolean docker, Path configPath, boolean ignoreShardRouting,
                       String nrts, String tlogs, String pulls, String shards, boolean skipSeedKafka) throws IOException {
    log.info("Loading test documents");
    if (docsPath != null) {
      docs = new TestDocumentCreator(mapper.readValue(docsPath.toFile(), new TypeReference<List<SolrDocument>>() {}));
    } else {
      docs = new TestDocumentCreator(NUM_DOCS);
    }

    this.shards = shards;
    this.nrts = nrts;
    this.collectionName = collectionName;
    this.kafkaHostPath = docker ? "kafka" : "localhost";
    this.tlogs = tlogs;
    if (configPath == null) {
      this.configPath = ignoreShardRouting
          ? Path.of("/opt/solr/server/solr/configsets/testconfig/conf/solrconfig-routing.xml")
          : Path.of("/opt/solr/server/solr/configsets/testconfig/conf/solrconfig.xml");
    } else {
      this.configPath = configPath;
    }
    this.pulls = pulls;
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
        ignoreShardRouting = true;
      } else {
        log.fatal("Unknown param passed, usage: [-d] [-p DOCS_PATH] [-c CONFIG_PATH] [--cname COLLECTION_NAME] [-k]" +
            "[--pulls NUM_PULL_REPLICAS] [--tlogs NUM_TLOG_REPLICAS] [--nrts|-r NUM_NRT_REPLICAS] [-s NUM_SHARDS] [-i]");
      }
    }

    MultiNodeTest test = new MultiNodeTest(collectionName, docsPath, docker, configPath, ignoreShardRouting,
        nrts, tlogs, pulls, shards, skipSeedKafka);
    test.uploadConfig();
    test.manageImporter(true);
    try {
      test.runTest();
    } finally {
      test.manageImporter(false);
    }
  }

  /**
   * Uploads the provided solrconfig.xml at the container path and creates the collection with the provided parameters.
   */
  public void uploadConfig() throws IOException, KeeperException, URISyntaxException {
    // Tries to get ZK_HOST as an environment variable
    String zkAddr = System.getenv("ZK_HOST");
    Path temp = null;

    // Connects to Zookeeper using ZK_HOST or localhost
    try (SolrZkClient client = new SolrZkClient(zkAddr == null ? "localhost:2181" : zkAddr, 30000)) {
      // Determine if the configuration is already present, and if not, copies the _default to solrkafka and replaces the solrconfig.xml
      if (!client.exists("/configs/solrkafka", false)) {
        temp = Files.createTempDirectory("solrkafka-test");
        client.downConfig("_default", temp);
        client.upConfig(temp, "solrkafka");
      }
      byte[] data = Files.readAllBytes(configPath);
      client.setData("/configs/solrkafka/solrconfig.xml", data, true);
    } catch (InterruptedException e) {
      throw new IOException(e);
    } finally {
      // Cleans up the temp directory if it was used
      if (temp != null) {
        FileUtils.forceDelete(temp.toFile());
      }
    }

    // Creates the collection with the provided parameters
    URIBuilder bldr = new URIBuilder(solrHostPath + "admin/collections")
        .setParameter("action", "CREATE").addParameter("numShards", shards)
        .addParameter("router.name", "compositeId").addParameter("nrtReplicas", nrts)
        .addParameter("tlogReplicas", tlogs).addParameter("pullReplicas", pulls)
        .addParameter("maxShardsPerNode", "-1").addParameter("name", collectionName)
        .addParameter("collection.configName", "solrkafka");
    // Makes the request, but doesn't throw an exception if a 400 is returned (signifies the collection already exists)
    makeRequest(new HttpGet(bldr.build()));
  }

  /**
   * Make a request and throw an exception if any status code other than 200 is returned.
   *
   * @param req The request to make
   * @return The body of the request
   */
  public String makeRequest(HttpUriRequest req) throws IOException {
    return makeRequest(req, 200);
  }

  /**
   * Make a request and throw an exception if any status codes other than the {@code expectedStatuses} are returned.
   *
   * @param req The request to make
   * @param expectedStatuses Status codes that can be returned and not throw an exception for
   * @return The body of the request
   */
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

  /**
   * Manages the importer by starting or stopping it. Sets {@link this#leader} to the first replica leader found
   * so that other methods can refer to the leader that the importer is started on.
   *
   * @param start {@code true} if the importer should be started, {@link false} if it should be stopped
   */
  public void manageImporter(boolean start) throws IOException {
    if (leader == null) {
      HttpGet get = new HttpGet(solrHostPath + "admin/cores?action=STATUS");
      String bodyString = makeRequest(get);
      JsonNode body = mapper.readTree(bodyString);

      String nodeName = null;
      for (JsonNode node : body.get("status")) {
        // Skip this node if it's part of the wrong collection
        if (!node.get("cloud").get("collection").textValue().equals(collectionName)) {
          continue;
        }
        nodeName = node.get("name").textValue();
        log.info("{} SolrKafka importer", start ? "starting" : "stopping");
        get = new HttpGet(solrHostPath + nodeName + pluginEndpoint + (start ? "" : "?action=stop"));
        bodyString = makeRequest(get);
        body = mapper.readTree(bodyString);
        // If this node is not a leader, start another replica instead
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

  /**
   * Send a request to the leader node to force a commit.
   */
  public void forceCommit() throws IOException {
    log.info("Forcing commit");
    HttpGet get = new HttpGet(solrHostPath + leader + "/update?commit=true");
    makeRequest(get);
  }

  /**
   * Check if the doc count in each of the replicas is as expected and make sure all nodes are still active.
   *
   * @param numRecords The number of records that are expected to be found
   */
  private void checkDocCount(int numRecords) throws IOException {
    if (numRecords > 0) {
      log.info("Sleeping for 5 seconds to let commit complete");
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        throw new IllegalStateException("Interrupted while waiting", e);
      }
    }

    HttpGet get = new HttpGet(solrHostPath + "admin/collections?action=CLUSTERSTATUS&collection=" + collectionName);
    String bodyString = makeRequest(get);
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

    // Try to check the number of documents each node contains up to 5 times
    for (int i = 0; i < 25; i++) {
      errors.clear();
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        throw new IllegalStateException("Interrupted while waiting", e);
      }

      // Select all docs to determine if the correct number of documents overall are found if not ignoring shard routing
      if (!ignoreShardRouting) {
        get = new HttpGet(solrHostPath + leader + "/select?q=*:*&rows=0");
        bodyString = makeRequest(get);
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
      bodyString = makeRequest(get);
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
    }
    throw new IllegalStateException("Errors found in state after 5 rounds of requests");
  }

  /**
   * Run the test by confirming doc count is 0, seeding Kafka (if applicable), waiting for the importer to catch up,
   * and checking the doc count.
   */
  public void runTest() throws IOException {
    forceCommit();
    checkDocCount(0);

    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaHostPath + kafkaPort);
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "MultiNodeTestProducer");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, SolrDocumentSerializer.class.getName());

    final long start = System.currentTimeMillis();
    if (!skipSeedKafka) {
      log.info("Sending {} documents to topic {}", docs.size(), topic);
      try (Producer<String, SolrDocument> producer = new KafkaProducer<>(props)) {
        for (SolrDocument doc : docs) {
          ProducerRecord<String, SolrDocument> record = new ProducerRecord<>(topic, doc.get("id").toString(), doc);
          producer.send(record);
        }
      }
      log.info("Done sending documents, waiting until consumer lag is 0");
    } else {
      log.info("Skip seed Kafka");
    }

    waitForLag(docs.size());
    final double duration = (System.currentTimeMillis() - start) / 1000.0;
    forceCommit();
    checkDocCount(docs.size());
    log.info("Duration for {} docs was {} seconds, {} docs / second", docs.size(), duration,
        docs.size() / duration);
    log.info("Test passed");
  }

  /**
   * Waits for the consumer group lag to be 0 for each Kafka partition or the importer to stop.
   * If it's not reached in 45 seconds, an execption is thrown.
   */
  public void waitForLag(int numDocs) throws IOException {
    HttpGet get = new HttpGet(solrHostPath + leader + pluginEndpoint + "?action=status");
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

      String bodyString = makeRequest(get);
      JsonNode body = mapper.readTree(bodyString);

      // If the importer is stopped, return
      if (body.has("status") && body.get("status").textValue().equals("STOPPED")) {
        log.info("Status is stopped, checking core state");
        return;
      }

      // Check consumer_group_lag
      if (body.has("consumer_group_lag") && body.get("consumer_group_lag").isObject()) {
        JsonNode lag = body.get("consumer_group_lag");
        boolean finished = true;
        // If there are no entries, try again
        if (lag.size() == 0) {
          continue;
        }

        int offsetSums = 0;
        // Check each partition, if the partition's lag is > 0, retry
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

        // We've caught up, so return
        return;
      }
    }
  }
}
