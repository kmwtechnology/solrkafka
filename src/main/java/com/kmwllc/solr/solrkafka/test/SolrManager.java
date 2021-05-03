package com.kmwllc.solr.solrkafka.test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FileUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.zookeeper.KeeperException;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class SolrManager implements AutoCloseable {
  private static final Logger log = LogManager.getLogger(SolrManager.class);
  private static final String pluginEndpoint = "/kafka";
  private final ObjectMapper mapper;
  private final CloseableHttpClient client = HttpClients.createDefault();
  private final File workingDir;
  private final String collectionName;
  private final String solrHostPath;

  public SolrManager(String solrHostPath, String collectionName, Path workingDir, ObjectMapper mapper) {
    this.solrHostPath = solrHostPath;
    this.collectionName = collectionName;
    this.mapper = mapper;

    if (workingDir == null) {
      workingDir = Path.of("/opt/solr/bin/");
    }
    this.workingDir = workingDir.toFile();
  }

  @Override
  public void close() throws IOException {
    manageImporter(false);
    client.close();
  }

  public void stopSolr(String port) throws IOException {
    ProcessBuilder builder = new ProcessBuilder("./solr", "stop", "-p", port);
    builder.directory(workingDir);
    builder.inheritIO();
    Process proc = builder.start();
    try {
      if (0 != proc.onExit().get(60, TimeUnit.SECONDS).exitValue()) {
        throw new IllegalStateException("Unsuccessful exit code received: " + proc.exitValue());
      }
    } catch (InterruptedException | ExecutionException e) {
      throw new IllegalStateException("Exception occurred during process", e);
    } catch (TimeoutException e) {
      proc.destroyForcibly();
      throw new IllegalStateException("Process took too long to finish");
    }
  }

  /**
   * Uploads the provided solrconfig.xml at the container path and creates the collection with the provided parameters.
   */
  public void uploadConfigAndCreateCollection(String shards, String nrts, String pulls, String tlogs, Path configPath)
      throws IOException, KeeperException, URISyntaxException {
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
    CompletableFuture<CloseableHttpResponse> thread = CompletableFuture.supplyAsync(() -> {
      try {
        return client.execute(req);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    });
    try (CloseableHttpResponse res = thread.get(30000, TimeUnit.MILLISECONDS);
         BufferedInputStream entity = new BufferedInputStream(res.getEntity().getContent())) {
      if (!Arrays.asList(expectedStatuses).contains(res.getStatusLine().getStatusCode())) {
        log.error("Invalid response received: {}", new String(entity.readAllBytes()));
        throw new IllegalStateException("Unexpected status code received: " + res.getStatusLine().getStatusCode());
      }
      String body = new String(entity.readAllBytes());
      log.info("Received response: {}", body);
      return body;
    } catch (TimeoutException | InterruptedException e) {
      log.error("Concurrent exception while executing request", e);
      throw new IllegalStateException(e);
    } catch (ExecutionException e) {
      if (e.getCause() instanceof UncheckedIOException) {
        throw new IOException(e);
      }
      throw new IllegalStateException(e);
    }
  }

  /**
   * Manages the importer by starting or stopping it.
   *
   * @param start {@code true} if the importer should be started, {@link false} if it should be stopped
   */
  public void manageImporter(boolean start) throws IOException {
    log.info("{} SolrKafka importer", start ? "starting" : "stopping");
    HttpGet get = new HttpGet(solrHostPath + collectionName + pluginEndpoint + (start ? "" : "?action=stop"));
    makeRequest(get);
  }

  /**
   * Send a request to the leader node to force a commit.
   */
  public void forceCommit() throws IOException {
    log.info("Forcing commit");
    HttpGet get = new HttpGet(solrHostPath + collectionName + "/update?commit=true");
    makeRequest(get);
  }

  /**
   * Waits for the consumer group lag to be 0 for each Kafka partition or the importer to stop.
   * If it's not reached in 45 seconds, an execption is thrown.
   */
  public void waitForLag(int numDocs) throws IOException {
    HttpGet get = new HttpGet(solrHostPath + collectionName + pluginEndpoint + "?action=status");
    int round = 0;
    int numStatic = 0;
    int lastTotalOffset = numDocs;
    while (true) {
      try {
        if (numStatic > 5) {
          throw new IllegalStateException("Waited " + numStatic +
              " rounds, but documents could not all be consumed from Kafka");
        }
        log.info("Sleeping for 15 seconds on round {}", round++);
        Thread.sleep(15000);
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
          if (offsetSums == lastTotalOffset) {
            numStatic++;
          }
          lastTotalOffset = offsetSums;
          continue;
        }

        // We've caught up, so return
        return;
      }
    }
  }
}
