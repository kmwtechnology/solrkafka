package com.kmwllc.solr.solrkafka.handler.requesthandler;

import com.kmwllc.solr.solrkafka.importer.KafkaImporter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.core.CloseHook;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.AuthorizationContext;
import org.apache.solr.security.PermissionNameProvider;
import org.apache.solr.util.circuitbreaker.CircuitBreaker;
import org.apache.solr.util.circuitbreaker.CircuitBreakerManager;
import org.apache.solr.util.plugin.PluginInfoInitialized;
import org.apache.solr.util.plugin.SolrCoreAware;

import java.util.List;
import java.util.Map;

/**
 * A handler to start (or confirm the start of) the SolrKafka plugin. Creates a new {@link KafkaImporter},
 * and begins their processing in a separate thread (so that the request
 * doesn't depend on finishing the import).
 */
public class SolrKafkaRequestHandler extends RequestHandlerBase implements SolrCoreAware, PluginInfoInitialized, PermissionNameProvider {
  private static final Logger log = LogManager.getLogger(SolrKafkaRequestHandler.class);
  private SolrCore core;
  private KafkaImporter importer;
  private String incomingDataType = "solr";
  private long commitInterval = 5000;
  private volatile boolean shouldRun = false;
  private boolean ignoreShardRouting = false;
  private String topicName = null;
  private String kafkaBroker = null;

  public SolrKafkaRequestHandler() {
    log.info("Kafka Request Handler created.");
  }

  /**
   * Handle the request to start the Kafka consumer by ensuring no {@link CircuitBreaker}s have been tripped. If
   * a consumer is already running, avoids starting another one and returns an
   * 'already running' status.
   *
   * @param req The request received
   * @param rsp The response that will be returned
   */
  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) {

    // Ends this request if a circuit breaker is fired
    CircuitBreakerManager circuitBreakerManager = req.getCore().getCircuitBreakerManager();
    List<CircuitBreaker> breakers = circuitBreakerManager.checkTripped();
    if (breakers != null) {
      String errorMsg = CircuitBreakerManager.toErrorMessage(breakers);
      rsp.add(CommonParams.STATUS, CommonParams.FAILURE);
      rsp.setException(new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE, "Circuit Breakers tripped " + errorMsg));
      return;
    }

    if (core.getCoreDescriptor().getCloudDescriptor() == null && this.ignoreShardRouting) {
      String msg = "Ignore shard routing set to true, but cluster is not running in cloud mode";
      rsp.add("message", msg + ". Run in cloud mode to use this feature.");
      rsp.setException(new IllegalStateException(msg));
      return;
    }

    rsp.add("status",
        importer == null ? "NOT_INITIALIZED" :
            importer.isRunning() ? "RUNNING" : "STOPPED");
    if (importer != null && importer.isThreadAlive()) {
      Map<String, Long> consumerGroupLag = importer.getConsumerGroupLag();
      rsp.add("consumer_group_lag", consumerGroupLag);
    }

    // Determines if this is the current leader and adds that information to the response
    boolean isLeader;
    try {
      isLeader = isCoreLeader(core);
      rsp.add("leader", isLeader);
    } catch (InterruptedException e) {
      log.error("Interrupted while determining core leader status", e);
      rsp.add("message", "Could not determine core leader status, exiting");
      return;
    }

    if (topicName == null || kafkaBroker == null) {
      rsp.add("message", "No topic or broker provided in solrconfig.xml!");
      rsp.setException(new IllegalStateException("No topic provided in solrconfig"));
      return;
    }

    // Gets the desired action, or uses "start" if none is supplied
    Object actionObj = req.getParams().get("action");
    String action;
    if (actionObj == null) {
      action = "start";
    } else {
      action = actionObj.toString();
    }

    // If the start action is supplied, setup and maybe start the importer
    if (action.equalsIgnoreCase("start")) {
      shouldRun = true;
      // Starts the importer if this is the leader
      if (isLeader) {
        if (!startImporter()) {
          rsp.add("message", "Request already running");
          rsp.add("running", true);
          return;
        }

        rsp.add("status", "started");
        rsp.add("running", true);
        return;
      } else {
        log.info("Not leader, ready to run");
        rsp.add("message", "Not leader, but ready to run");
        rsp.add("running", false);
      }
      return;
    }

    // Exits if the importer is not running and we're the leader. All commands below require a running importer if leader.
    if (isLeader && !shouldRun && (importer == null || !importer.isRunning())) {
      rsp.add("status", "SolrKafka not running");
      rsp.add("running", false);
      return;
    }

    // Handle the provided action
    if (action.equalsIgnoreCase("stop")) {
      shouldRun = false;
      if (isLeader) {
        importer.stop();
      }
      rsp.add("status", "Stopping SolrKafka");
      rsp.add("running", false);
    } else if (!isLeader) {
      rsp.add("status", "Core is not leader");
      rsp.add("running", shouldRun);
    } else if (!action.equalsIgnoreCase("status")) {
      rsp.add("status", "Unknown command provided");
      rsp.add("running", true);
    }
  }

  /**
   * Sets up and starts a new importer.
   *
   * @return {@code true} if an importer was created and started, {@link false} if one was already running
   */
  private boolean startImporter() {
    // Exit if there's already an importer running
    if (importer != null && importer.isThreadAlive()) {
      log.info("Importer already running, skipping start process");
      return false;
    }

    // Stops any previously running importer and closes resources
    if (importer != null && importer.isRunning()) {
      log.info("Stopping previously running importer");
      importer.stop();
    }

    // Create the importer
    importer = new KafkaImporter(core, kafkaBroker, topicName, commitInterval,
        ignoreShardRouting, incomingDataType);


    // Sets up the status handler and starts the importer
    importer.startThread();
    return true;
  }

  @Override
  public String getDescription() {
    return "Loading documents from Kafka";
  }

  @Override
  public Boolean registerV2() {
    return Boolean.TRUE;
  }

  @Override
  public Name getPermissionName(AuthorizationContext request) {
    return Name.READ_PERM;
  }

  @Override
  public void init(PluginInfo info) {
    init(info.initArgs);

    log.info("Initializing SolrKafkaRequestHandler with {} configs", info.initArgs);

    // Sets up configurations from solrconfig.xml (the defaults section from the requestHandler setup)
    Object incomingDataType = info.initArgs.findRecursive("defaults", "incomingDataType");
    Object commitInterval = info.initArgs.findRecursive("defaults", "commitInterval");
    Object ignoreShardRouting = info.initArgs.findRecursive("defaults", "ignoreShardRouting");
    Object topicName = info.initArgs.findRecursive("defaults", "topicName");
    Object kafkaBroker = info.initArgs.findRecursive("defaults", "kafkaBroker");

    // If the values from the defaults section are present, override
    if (incomingDataType != null) {
      this.incomingDataType = incomingDataType.toString();
    }
    if (commitInterval != null) {
      this.commitInterval = Long.parseLong(commitInterval.toString());
    }
    if (ignoreShardRouting != null) {
      this.ignoreShardRouting = Boolean.parseBoolean(ignoreShardRouting.toString());
    }
    if (topicName != null) {
      this.topicName = topicName.toString();
    }
    if (kafkaBroker != null) {
      this.kafkaBroker = kafkaBroker.toString();
    }
  }

  /**
   * Determines if the given {@link SolrCore} is the leader of its replicas. If Solr is not run in cloud mode,
   * it will always return true.
   *
   * @param core The {@link SolrCore} to check
   * @return {@code true} if the core is the leader of its replicas
   * @throws InterruptedException if an error occurred while contacting Zookeeper
   */
  public static boolean isCoreLeader(SolrCore core) throws InterruptedException {
    CloudDescriptor cloud = core.getCoreDescriptor().getCloudDescriptor();
    return cloud == null || core.getCoreContainer().getZkController().getZkStateReader().getLeaderRetry(
        cloud.getCollectionName(), cloud.getShardId()).getName().equals(cloud.getCoreNodeName());
  }

  @Override
  public void inform(SolrCore core) {
    log.info("New SolrCore provided");

    this.core = core;

    // Stop the currently running importer
    if (importer != null) {
      log.info("Setting new core in importer");
      importer.stop();
    }

    // If the core is a leader and we've been set up to run, start running
    try {
      if (isCoreLeader(core) && shouldRun) {
        startImporter();
      }
    } catch (InterruptedException e) {
      log.error("Interrupted while determining leader status", e);
      importer.stop();
    }

    // Add hook to stop the importer when the core is shutting down
    core.addCloseHook(new CloseHook() {
      @Override
      public void preClose(SolrCore core) {
        log.info("SolrCore shutting down");
        if (importer != null) {
          importer.stop();
        }
      }

      @Override
      public void postClose(SolrCore core) { }
    });
  }
}
