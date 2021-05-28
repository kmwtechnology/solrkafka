package com.kmwllc.solr.solrkafka.handler.requesthandler;

import com.kmwllc.solr.solrkafka.importer.KafkaImporter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
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
import java.time.Duration;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * A handler to start (or confirm the start of) the SolrKafka plugin. Creates a new {@link KafkaImporter},
 * and begins their processing in a separate thread (so that the request
 * doesn't depend on finishing the import).
 */
public class SolrKafkaRequestHandler extends RequestHandlerBase
    implements SolrCoreAware, PluginInfoInitialized, PermissionNameProvider, Watcher {

  /** Logger. */
  private static final Logger log = LogManager.getLogger(SolrKafkaRequestHandler.class);

  /** The default Zookeeper path name, used in the {@link this#zkCollectionPath}. */
  public static final String ZK_PLUGIN_PATH = "/solrkafka";

  /** The path of this importer in Zookeeper. This should be set when {@link this#inform(SolrCore)} is called. */
  private String zkCollectionPath = "";

  /** The current {@link SolrCore} being used. */
  private SolrCore core;

  /** The {@link KafkaImporter} currently running (or last run). */
  private KafkaImporter importer;

  /**
   * The incoming datatype from the Kafka topic. Corresponds to the
   * {@link org.apache.kafka.common.serialization.Deserializer} that will be returned from the
   * {@link com.kmwllc.solr.solrkafka.datatype.SerdeFactory}. Default is "solr" (for
   * {@link com.kmwllc.solr.solrkafka.datatype.solr.SolrDocumentDeserializer}).
   */
  private String incomingDataType = "solr";

  /** The minimum time between offset commits in ms back to Kafka. Default is 5000. */
  private long commitInterval = 5000;

  /**
   * Whether or not this should be running. Can be in an inconsistent state when the {@link SolrCore} is shutting
   * down or if there's an error.
   */
  private volatile boolean shouldRun = false;

  /** True if all shard routing should be performed. */
  private boolean ignoreShardRouting = false;

  /** The topics that the {@link KafkaImporter} should pull from. */
  private List<String> topicNames = null;

  /** The Kafka broker connection URL. */
  private String kafkaBroker = null;

  /** The {@link ZooKeeper} that should be used for making updates to the state of teh {@link KafkaImporter}. */
  private volatile ZooKeeper keeper;

  /** True if the Zookeeper state has already been set up and should not be run again. */
  private boolean hasBeenSetup = false;

  /**
   * The max poll interval in ms before the Kafka consumer should be purged. Should be larger than the
   * {@link this#commitInterval} Default is 45000.
   */
  private int kafkaPollInterval = 45000;

  /**
   * The maximum number of documents to be returned by the Kafka consumer in a single call to
   * {@link org.apache.kafka.clients.consumer.Consumer#poll(Duration)}. Default is 100.
   */
  private int kafkaMaxPollRecords = 100;

  /**
   * True if the Kafka consumer should start polling from the earliest offset where no existing offsets are found.
   * False if it should start from the latest offset.
   */
  private boolean autoOffsetResetBeginning = true;

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
    // Returns the name of the current core in the response
    rsp.add("current_core", core.getName());

    // Adds the status of the importer in the response
    rsp.add("status",
        importer == null ? "NOT_INITIALIZED" :
            importer.isRunning() ? "RUNNING" : "STOPPED");

    // Adds the consumer group lag for every partition in the topic, as well as the total lag, max lag, and most
    // behind partition
    Map<String, Long> consumerGroupLag = KafkaImporter.getConsumerGroupLag(kafkaBroker, topicNames);
    rsp.add("consumer_group_lag", consumerGroupLag);
    rsp.add("total_lag", consumerGroupLag.values().stream().mapToLong(l -> l).sum());
    Optional<Map.Entry<String, Long>> maxPartition =
        consumerGroupLag.entrySet().stream().max(Comparator.comparingLong(Map.Entry::getValue));
      if (maxPartition.isPresent()) {
        rsp.add("max_partition_lag", maxPartition.get().getValue());
        rsp.add("most_behind_partition", maxPartition.get().getKey());
      }

    rsp.add("shouldRun", shouldRun);

    // Determines if this is the current leader/the core is eligible to run and adds that information to the response
    try {
      boolean isLeader = isCoreLeader(core);
      rsp.add("leader", isLeader);
      boolean isEligible = isCoreEligible();
      rsp.add("eligible", isEligible);
    } catch (InterruptedException e) {
      log.error("Interrupted while determining core leader status", e);
      rsp.add("message", "Could not determine core leader status, exiting");
      return;
    }

    if (topicNames == null || kafkaBroker == null) {
      rsp.add("message", "No topic or broker provided in solrconfig.xml!");
      rsp.setException(new IllegalStateException("No topic provided in solrconfig"));
      return;
    }

    // Gets the desired action, or uses "start"/"status" if none is supplied
    Object actionObj = req.getParams().get("action");
    String action;
    if (actionObj == null) {
      action = shouldRun ? "status" : "start";
    } else {
      action = actionObj.toString();
    }

    // If the start action is supplied, setup and maybe start the importer
    if (action.equalsIgnoreCase("start")) {
      // Returns an exception if the cluster is in an invalid state for the importer to start
      if ((core.getCoreDescriptor().getCloudDescriptor() == null || clusterContainsTlogs()) && ignoreShardRouting) {
        String msg = "Ignore shard routing set to true, but cluster is not running in cloud mode or a replica type is TLOG";
        rsp.add("message", msg + ". Run in cloud mode to use this feature or change replica type.");
        rsp.setException(new IllegalStateException(msg));
        log.error(msg);
        return;
      }

      // Ends this request if a circuit breaker is fired
      CircuitBreakerManager circuitBreakerManager = req.getCore().getCircuitBreakerManager();
      List<CircuitBreaker> breakers = circuitBreakerManager.checkTripped();
      if (breakers != null) {
        String errorMsg = CircuitBreakerManager.toErrorMessage(breakers);
        rsp.add(CommonParams.STATUS, CommonParams.FAILURE);
        rsp.setException(new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE, "Circuit Breakers tripped " + errorMsg));
        return;
      }

      // Starts the importer
      if (changeRunningState(true)) {
        rsp.add("message", "started");
        rsp.add("running", true);
      } else {
        rsp.add("message", "Could not start, check logs for info");
        rsp.setException(new IllegalStateException("Failed to start Zookeeper"));
      }
      return;
    }

    // Handle the provided action
    if (action.equalsIgnoreCase("stop")) {
      if (changeRunningState(false)) {
        rsp.add("message", "stopping");
        rsp.add("running", false);
      } else {
        rsp.add("message", "Failed to stop, check logs for info");
        rsp.setException(new IllegalStateException("Failed to stop Zookeeper"));
      }
    } else if (!action.equalsIgnoreCase("status")) {
      rsp.add("message", "Unknown command provided");
    }
  }

  /**
   * Changes the state of the plugin depending on the value of the {@code start} param. If the cluster is running
   * in cloud mode, functionality is performed through Zookeeper and handled in the {@link Watcher#process(WatchedEvent)}
   * method. If the cluster is not in cloud mode, the {@link this#importer} is simply started or stopped.
   *
   * @param start True if the importer should be started, false if it should be stopped
   * @return True if the action was a success, false otherwise
   */
  private boolean changeRunningState(boolean start) {
    final String state = start ? "RUNNING" : "STOPPED";

    if (core.getCoreDescriptor().getCloudDescriptor() != null) {
      // We're running in cloud mode, contact Zookeeper
      log.info("Changing running state to {} in cloud mode", state);
      // Try up to 10 times to change Zookeeper state, otherwise just give up
      for (int i = 0; i < 10; i++) {
        // Get ZNode status info and try to update node state value
        try {
          Stat stat = keeper.exists(zkCollectionPath, false);
          keeper.setData(zkCollectionPath, state.getBytes(StandardCharsets.UTF_8),
              stat.getVersion());
          return true;
        } catch (InterruptedException e) {
          log.error("Interrupted, retrying", e);
        } catch (KeeperException e) {
          log.error("ZK error encountered, trying set again", e);
        }
        try {
          Thread.sleep(1);
        } catch (InterruptedException e) {
          log.error("Interrupted", e);
        }
      }
      log.info("Failed to change state after 10 attempts");
      return false;
    }

    // We're not running in cloud mode, simply change state
    shouldRun = start;
    log.info("Changing running state to {} in normal (non-cloud) mode", state);
    if (start) {
      startImporter();
    } else {
      importer.stop();
    }
    return true;
  }

  /**
   * Sets up and starts a new importer.
   */
  private synchronized void startImporter() {
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
    importer = new KafkaImporter(core, kafkaBroker, topicNames, commitInterval,
        ignoreShardRouting, incomingDataType, kafkaPollInterval, autoOffsetResetBeginning, kafkaMaxPollRecords);


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
    Object topicNames = info.initArgs.findRecursive("defaults", "topicNames");
    Object kafkaBroker = info.initArgs.findRecursive("defaults", "kafkaBroker");
    Object kafkaPollInterval = info.initArgs.findRecursive("defaults", "kafkaPollInterval");
    Object autoOffsetResetConfig = info.initArgs.findRecursive("defaults", "autoOffsetResetConfig");
    Object kafkaPollRecords = info.initArgs.findRecursive("defaults", "kafkaPollRecords");

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
    if (topicNames != null) {
      this.topicNames = Arrays.asList(topicNames.toString().split(","));
    }
    if (kafkaBroker != null) {
      this.kafkaBroker = kafkaBroker.toString();
    }
    if (kafkaPollInterval != null) {
      this.kafkaPollInterval = Integer.parseInt(kafkaPollInterval.toString());
    }
    if (autoOffsetResetConfig != null) {
      this.autoOffsetResetBeginning = autoOffsetResetConfig.toString().equalsIgnoreCase("beginning");
    }
    if (kafkaPollRecords != null) {
      this.kafkaMaxPollRecords = Integer.parseInt(kafkaPollRecords.toString());
    }
  }

  /**
   * Returns true if the core is eligible to run. This occurs when the node is a leader, is an
   * NRT type, or if the cluster is not running in cloud mode.
   *
   * @return True if the plugin should run on the core
   * @throws InterruptedException if {@link this#isCoreLeader(SolrCore)} throws
   */
  public boolean isCoreEligible() throws InterruptedException {
    CloudDescriptor cloud = core.getCoreDescriptor().getCloudDescriptor();
    if (cloud == null || !ignoreShardRouting || cloud.getReplicaType() == Replica.Type.NRT) {
      return true;
    }

    return isCoreLeader(core);
  }

  /**
   * Determines if the cluster currently contains TLOGs.
   *
   * @return True if any replicas in the cluster are TLOGs
   */
  public boolean clusterContainsTlogs() {
    CloudDescriptor cloud = core.getCoreDescriptor().getCloudDescriptor();
    if (cloud == null) {
      return false;
    }
    String collectionName = cloud.getCollectionName();
    ClusterState cluster = core.getCoreContainer().getZkController().getClusterState();
    DocCollection coll = cluster.getCollection(collectionName);
    return coll.getReplicas().stream().anyMatch(replica -> replica.getType() == Replica.Type.TLOG);
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
    try {
      log.info("New SolrCore provided");

      this.core = core;
      CloudDescriptor cloud = core.getCoreDescriptor().getCloudDescriptor();

      if (cloud != null && !hasBeenSetup) {
        // We're running in cloud mode and haven't set up Zookeeper yet
        zkCollectionPath = ZK_PLUGIN_PATH + "-" + cloud.getCollectionName();
        SolrZkClient client = core.getCoreContainer().getZkController().getZkClient();
        keeper = client.getSolrZooKeeper();

        int i = 0;
        // If we're a PULL replica, don't run this since we don't need to watch anything and a leader replica will
        // set it up for us
        while (cloud.getReplicaType() != Replica.Type.PULL) {
          try {
            try {
              // Try to set status to stopped
              client.create(zkCollectionPath, "STOPPED".getBytes(StandardCharsets.UTF_8),
                  CreateMode.PERSISTENT, true);
              log.info("ZK plugin node created");
            } catch (KeeperException.NodeExistsException e) {
              // The node already existed, don't care about the node's value yet...
              log.info("ZK plugin node not originally found but has been created externally, skipping creation", e);
            }

            // TODO: if issues come up with re-establishing a watch during node-reconnect, check out apache curator
            // Add myself as a watcher to the node at zkCollectionPath (we know it exists because we made it through
            // the previous try block
            keeper.addWatch(zkCollectionPath, this, AddWatchMode.PERSISTENT_RECURSIVE);
            Stat stat = keeper.exists(zkCollectionPath, false);

            // If the value of the block is currently "RUNNING", then we should start running
            shouldRun = new String(keeper.getData(zkCollectionPath, false, stat), StandardCharsets.UTF_8)
                .trim().equals("RUNNING");
            log.info("Created {}/{} node and watcher, shouldRun = {}", zkCollectionPath, core.getName(), shouldRun);
            // We're finished setting up so break out of the loop
            break;
          } catch (InterruptedException e) {
            log.error("Interrupted while setting up cloud mode", e);
            break;
          } catch (KeeperException e) {
            log.error("Error occurred while setting up Zookeeper state", e);
          }
          // Tried 10 times to start up, but couldn't
          if (++i > 10) {
            // TODO: should this throw a SolrException?
            log.error("Could not initialize ZK client");
            return;
          } else {
            try {
              // Sleep for 100 milliseconds before retrying
              Thread.sleep(100);
            } catch (InterruptedException e) {
              log.error("Interrupted while setting up cloud mode", e);
              break;
            }
          }
        }
      }
      // Set this so we don't try to set up again
      hasBeenSetup = true;

      // Probably won't be run concurrently, but still want to make sure we don't lose a reference to a
      // (possibly) running KafkaImporter
      synchronized (this) {
        // Stop the currently running importer
        if (importer != null && importer.isRunning()) {
          log.info("Setting new core in importer");
          importer.stop();
        }

        try {
          // If we've been set up to run, start running
          if (shouldRun && isCoreEligible()) {
            startImporter();
          } else if (!isCoreEligible()) {
            log.info("Not starting importer because core is not eligible to start");
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    } catch(Throwable e){
      log.fatal("SolrKafkaRequestHandler could not be set up", e);
    }
  }

  @Override
  public void process(WatchedEvent event) {
    log.info("ZK watcher callback event received in core {} for event {}", core.getName(), event);
    // If the event isn't at the expected zkCollectionPath node or it's not a data changed event, then ignore
    if (event.getPath() == null || !event.getPath().equals(zkCollectionPath)
        || event.getType() != Event.EventType.NodeDataChanged) {
      log.info("Incorrect event path or type");
      return;
    }

    // get the data at the node and determine what to do in callback
    keeper.getData(zkCollectionPath, false, (rc, path, ctx, data, stat) -> {
      // If status isn't OK, log error and stop importer
      if (rc != KeeperException.Code.OK.intValue()) {
        log.error("Non-OK code received in ZK callback for core {}: {}, stopping importer", core.getName(), rc);
        if (importer != null && importer.isRunning()) {
          importer.stop();
        }
        return;
      }

      // Get data from node
      String text = new String(data, StandardCharsets.UTF_8).trim();

      // If data isn't RUNNING or STOPPED, log error and do nothing
      if (!(text.equalsIgnoreCase("RUNNING") || text.equalsIgnoreCase("STOPPED"))) {
        log.warn("Unknown status received from Zookeeper");
        return;
      }

      // Determine if we should run or not
      shouldRun = text.equalsIgnoreCase("RUNNING");

      // If we're not eligible to run, don't run (this shouldn't happen since we're not supporting TLOGs and don't
      // run on PULLs)
      try {
        if (!isCoreEligible()) {
          log.info("Received process event from ZK but core is not eligible to start");
          return;
        }
      } catch (InterruptedException e) {
        return;
      }


      // Start or stop importer
      if ((importer == null || !importer.isRunning()) && shouldRun) {
        log.info("Starting importer from ZK watch callback for core {}", core.getName());
        startImporter();
      } else if (importer != null && importer.isRunning() && !shouldRun) {
        log.info("Stopping importer from ZK watch callback for core {}", core.getName());
        importer.stop();
      }
    }, null);
  }
}
