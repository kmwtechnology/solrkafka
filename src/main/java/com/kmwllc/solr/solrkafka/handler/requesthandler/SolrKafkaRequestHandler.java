package com.kmwllc.solr.solrkafka.handler.requesthandler;

import com.kmwllc.solr.solrkafka.importer.KafkaImporter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.SolrZkClient;
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
import org.apache.zookeeper.AddWatchMode;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * A handler to start (or confirm the start of) the SolrKafka plugin. Creates a new {@link KafkaImporter},
 * and begins their processing in a separate thread (so that the request
 * doesn't depend on finishing the import).
 */
public class SolrKafkaRequestHandler extends RequestHandlerBase
    implements SolrCoreAware, PluginInfoInitialized, PermissionNameProvider, Watcher {
  private static final Logger log = LogManager.getLogger(SolrKafkaRequestHandler.class);
  public static final String ZK_PLUGIN_PATH = "/solrkafka";
  private SolrCore core;
  private KafkaImporter importer;
  private String incomingDataType = "solr";
  private long commitInterval = 5000;
  private volatile boolean shouldRun = false;
  private boolean ignoreShardRouting = false;
  private String topicName = null;
  private String kafkaBroker = null;
  private volatile ZooKeeper keeper;
  private volatile boolean hasBeenSetup = false;

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

    rsp.add("current_core", core.getName());

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
      rsp.add("total_offset", consumerGroupLag.values().stream().mapToLong(l -> l).sum());
    } else {
      rsp.add("consumer_group_lag", "STOPPED");
    }

    // Determines if this is the current leader and adds that information to the response
    try {
      boolean isLeader = isCoreLeader(core);
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
      action = shouldRun ? "status" : "start";
    } else {
      action = actionObj.toString();
    }

    // If the start action is supplied, setup and maybe start the importer
    if (action.equalsIgnoreCase("start")) {
      changeRunningState(true);

      rsp.add("status", "started");
      rsp.add("running", true);
      return;
    }

    // Exits if the importer is not running. All commands below require a running importer if leader.
    if (importer == null || !importer.isRunning()) {
      rsp.add("status", "SolrKafka not running");
      rsp.add("running", false);
      return;
    }

    // Handle the provided action
    if (action.equalsIgnoreCase("stop")) {
      changeRunningState(false);
      rsp.add("status", "Stopping SolrKafka");
      rsp.add("running", false);
    } else if (!action.equalsIgnoreCase("status")) {
      rsp.add("status", "Unknown command provided");
      rsp.add("running", true);
    }
  }

  private void changeRunningState(boolean start) {
    shouldRun = start;
    final String state = start ? "RUNNING" : "STOPPED";
    if (isCloudMode(core)) {
      log.info("Changing running state to {} in cloud mode", state);
      int i = 0;
      while (true) {
        try {
          Stat stat = keeper.exists(ZK_PLUGIN_PATH, false);
          keeper.setData(ZK_PLUGIN_PATH, state.getBytes(StandardCharsets.UTF_8),
              stat.getVersion());
          break;
        } catch (InterruptedException e) {
          log.error("Interrupted");
          return;
        } catch (KeeperException e) {
          log.error("ZK error encountered, trying set again", e);
        }
        if (i++ > 10) {
          log.info("Failed to change state after 10 attempts");
          return;
        }
      }
      return;
    }

    log.info("Changing running state to {} in normal (non-cloud) mode", state);
    if (start) {
      startImporter();
    } else {
      importer.stop();
    }
  }

  /**
   * Sets up and starts a new importer.
   */
  private void startImporter() {
    // Exit if there's already an importer running
    if (importer != null && importer.isThreadAlive()) {
      log.info("Importer already running, skipping start process");
      return;
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

  public static boolean isCloudMode(SolrCore core) {
    return core.getCoreDescriptor().getCloudDescriptor() != null;
  }

  @Override
  public void inform(SolrCore core) {
    log.info("New SolrCore provided");

    this.core = core;

    if (isCloudMode(core) && !hasBeenSetup) {
      int i = 0;
      SolrZkClient client = core.getCoreContainer().getZkController().getZkClient();
      keeper = client.getSolrZooKeeper();
      while (true)  {
        // TODO: will make a best attempt to not immediately begin running, but no promises
        try {
          Stat stat = keeper.exists(ZK_PLUGIN_PATH, false);
          try {
            if (stat != null) {
              log.info("ZK plugin node found, will not attempt to re-create");
              List<String> children = keeper.getChildren(ZK_PLUGIN_PATH, false);
              log.info("ZK plugin node children: {}", children);
              if (children.isEmpty()) {
                log.info("ZK plugin node has no children, making sure plugin is not running on start");
                keeper.setData(ZK_PLUGIN_PATH, "STOPPED".getBytes(StandardCharsets.UTF_8), stat.getVersion());
              }
            } else {
              client.create(ZK_PLUGIN_PATH, "STOPPED".getBytes(StandardCharsets.UTF_8),
                  CreateMode.CONTAINER, true);
              log.info("ZK plugin node created");
            }
          } catch (KeeperException.NodeExistsException e) {
            log.info("ZK plugin node not originally found but has been created externally, skipping creation");
          } catch (KeeperException.BadVersionException e) {
            log.info("ZK plugin status updated concurrently, skipping update");
          }

          client.create(ZK_PLUGIN_PATH + "/" + core.getName(), core.getName().getBytes(StandardCharsets.UTF_8),
              CreateMode.EPHEMERAL, true);
          shouldRun = new String(keeper.getData(ZK_PLUGIN_PATH, false, stat), StandardCharsets.UTF_8)
              .trim().equals("RUNNING");
          core.getCoreContainer().getZkController().getZkClient().getSolrZooKeeper()
              .addWatch(ZK_PLUGIN_PATH, this, AddWatchMode.PERSISTENT_RECURSIVE);
          log.info("Created {}/{} node and watcher, shouldRun = {}", ZK_PLUGIN_PATH, core.getName(), shouldRun);
          break;
        } catch (InterruptedException e) {
          log.error("Interrupted while setting up cloud mode", e);
          break;
        } catch (KeeperException e) {
          log.error("Error occurred while setting up Zookeeper state", e);
        }
        if (i++ > 10) {
          log.error("Could not initialize ZK client");
          return;
        }
      }
    }
    hasBeenSetup = true;

    // Stop the currently running importer
    if (importer != null) {
      log.info("Setting new core in importer");
      importer.stop();
    }

    // If we've been set up to run, start running
    if (shouldRun) {
      startImporter();
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

  @Override
  public void process(WatchedEvent event) {
    log.info("ZK watcher callback event received in core {}", core.getName());
    if (!event.getPath().equals(ZK_PLUGIN_PATH) || event.getType() != Event.EventType.NodeDataChanged) {
      return;
    }

    keeper.getData(ZK_PLUGIN_PATH, false, (rc, path, ctx, data, stat) -> {
      if (rc != KeeperException.Code.OK.intValue()) {
        log.error("Non-OK code received in ZK callback for core {}: {}, stopping importer", core.getName(), rc);
        if (importer != null && !importer.isRunning()) {
          importer.stop();
        }
        return;
      }

      String text = new String(data, StandardCharsets.UTF_8).trim();

      if (text.equalsIgnoreCase("RUNNING")
          && (importer == null || !importer.isRunning())) {
        log.info("Starting importer from ZK watch callback for core {}", core.getName());
        shouldRun = true;
        startImporter();
      } else if (text.equalsIgnoreCase("STOPPED")
          && (importer != null && importer.isRunning())) {
        log.info("Stopping importer from ZK watch callback for core {}", core.getName());
        shouldRun = false;
        if (importer != null) {
          importer.stop();
        }
      }
    }, null);
  }
}
