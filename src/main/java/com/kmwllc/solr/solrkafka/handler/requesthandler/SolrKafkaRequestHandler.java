package com.kmwllc.solr.solrkafka.handler.requesthandler;

import com.kmwllc.solr.solrkafka.importer.Importer;
import com.kmwllc.solr.solrkafka.importer.KafkaImporter;
import com.kmwllc.solr.solrkafka.importer.SolrDocumentImportHandler;
import com.kmwllc.solr.solrkafka.handler.consumerhandler.AsyncKafkaConsumerHandler;
import com.kmwllc.solr.solrkafka.handler.consumerhandler.KafkaConsumerHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.core.CloseHook;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.AuthorizationContext;
import org.apache.solr.security.PermissionNameProvider;
import org.apache.solr.util.circuitbreaker.CircuitBreaker;
import org.apache.solr.util.circuitbreaker.CircuitBreakerManager;
import org.apache.solr.util.plugin.PluginInfoInitialized;
import org.apache.solr.util.plugin.SolrCoreAware;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * A handler to start (or confirm the start of) the SolrKafka plugin. Creates a new {@link SolrDocumentImportHandler}
 * and {@link AsyncKafkaConsumerHandler}, and begins their processing in a separate thread (so that the request
 * doesn't depend on finishing the import).
 */
public class SolrKafkaRequestHandler extends RequestHandlerBase implements SolrCoreAware, PluginInfoInitialized, PermissionNameProvider {
  private static final Logger log = LogManager.getLogger(SolrKafkaRequestHandler.class);
  private SolrCore core;
  private static final String topic = "testtopic";
  private Importer importer;
  private final Properties initProps = new Properties();
  private String incomingDataType = "solr";
  private String consumerType = "sync";
  private long commitInterval = 5000;
  private volatile boolean shouldRun = false;

  public SolrKafkaRequestHandler() {
    log.info("Kafka Consumer created.");
  }

  /**
   * Handle the request to start the Kafka consumer by ensuring no {@link CircuitBreaker}s have been tripped. If
   * a {@link SolrDocumentImportHandler} is already running, avoids starting another one and returns an
   * 'already running' status.
   *
   * @param req The request received
   * @param rsp The response that will be returned
   */
  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) {
    // TODO: is this required?
    ResponseBuilder rb = new ResponseBuilder(req, rsp, new ArrayList<>());

    CircuitBreakerManager circuitBreakerManager = req.getCore().getCircuitBreakerManager();
    List<CircuitBreaker> breakers = circuitBreakerManager.checkTripped();
    if (breakers != null) {
      String errorMsg = CircuitBreakerManager.toErrorMessage(breakers);
      rsp.add(CommonParams.STATUS, CommonParams.FAILURE);
      rsp.setException(new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE, "Circuit Breakers tripped " + errorMsg));
      return;
    }

    // TODO: if distributed, use a shard handler to forward req (see distributedupdateprocessor and searchhandler)

    boolean isLeader;

    try {
      isLeader = isCoreLeader(core);
      if (isLeader) {
        rsp.add("leader", "true");
      } else {
        rsp.add("leader", "false");
      }
    } catch (InterruptedException e) {
      log.error("Interrupted while determining core leader status", e);
      rsp.add("Status", "Could not determine core leader status, exiting");
      return;
    }

    Object actionObj = req.getParams().get("action");
    String action;
    if (actionObj == null) {
      action = "start";
    } else {
      action = actionObj.toString();
    }

    if (action.equalsIgnoreCase("start")) {
      shouldRun = true;
      if (isLeader) {
        boolean fromBeginning = req.getParams().getBool("fromBeginning", false);
        boolean readFullyAndExit = req.getParams().getBool("exitAtEnd", false);

        if (!startImporter(fromBeginning, readFullyAndExit)) {
          rsp.add("Status", "Request already running");
          return;
        }

        rsp.add("Status", "Started");
        return;
      } else {
        log.info("Not leader, ready to run");
        rsp.add("Status", "Not leader, but ready to run");
      }
      return;
    }

    if (isLeader && (importer == null || !importer.isThreadAlive())) {
      rsp.add("Status", "SolrKafka not running");
      return;
    }

    if (action.equalsIgnoreCase("stop")) {
      shouldRun = false;
      if (isLeader) {
        importer.stop();
      }
      rsp.add("Status", "Stopping SolrKafka");
    } else if (!isLeader) {
      rsp.add("Status", "Core is not leader");
    } else if (action.equalsIgnoreCase("pause")) {
      importer.pause();
      rsp.add("Status", "Paused SolrKafka");
    } else if (action.equalsIgnoreCase("resume")) {
      importer.resume();
      rsp.add("Status", "Resumed SolrKafka");
    } else if (action.equalsIgnoreCase("rewind")) {
      importer.rewind();
      rsp.add("Status", "Rewound SolrKafka");
    }
  }

  private boolean startImporter(boolean fromBeginning, boolean readFullyAndExit) {
    if (importer != null && importer.isThreadAlive()) {
      log.info("Importer already running, skipping start process");
      return false;
    }

    if (importer != null) {
      log.info("Stopping previously running importer");
      importer.stop();
    }

    if (!consumerType.equalsIgnoreCase("simple")) {
      log.info("Creating {} KafkaConsumerHandler for SolrDocumentImportHandler Importer type", consumerType);
      KafkaConsumerHandler consumerHandler = KafkaConsumerHandler.getInstance(consumerType,
          initProps, topic, fromBeginning, readFullyAndExit, incomingDataType);
      importer = new SolrDocumentImportHandler(core, consumerHandler, commitInterval);
    } else {
      log.info("Creating KafkaImporter Importer type");
      importer = new KafkaImporter(core, readFullyAndExit, fromBeginning, commitInterval, false);
    }

    SolrKafkaStatusRequestHandler.setHandler(importer);
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

    Object consumerType = info.initArgs.findRecursive("defaults", "consumerType");
    Object incomingDataType = info.initArgs.findRecursive("defaults", "incomingDataType");
    Object commitInterval = info.initArgs.findRecursive("defaults", "commitInterval");

    if (consumerType != null) {
      this.consumerType = consumerType.toString();
    }
    if (incomingDataType != null) {
      this.incomingDataType = incomingDataType.toString();
    }
    if (commitInterval != null) {
      this.commitInterval = Long.parseLong(commitInterval.toString());
    }
  }

  public static boolean isCoreLeader(SolrCore core) throws InterruptedException {
    CloudDescriptor cloud = core.getCoreDescriptor().getCloudDescriptor();
    return core.getCoreContainer().getZkController().getZkStateReader().getLeaderRetry(
        cloud.getCollectionName(), cloud.getShardId()).getName().equals(cloud.getCoreNodeName());
  }

  @Override
  public void inform(SolrCore core) {
    log.info("New SolrCore provided");

    this.core = core;

    if (importer != null) {
      log.info("Setting new core in importer");
      importer.pause();
      importer.setNewCore(core);
    }

    try {
      if (isCoreLeader(core) && shouldRun) {
        if (importer == null || !importer.getStatus().isOperational()) {
          startImporter(false, false);
        } else {
          importer.resume();
        }
      }
    } catch (InterruptedException e) {
      log.error("Interrupted while determining leader status", e);
      importer.stop();
    }

    core.addCloseHook(new CloseHook() {
      @Override
      public void preClose(SolrCore core) {
        log.info("SolrCore shutting down");
        if (importer != null) {
          importer.pause();
        }
      }

      @Override
      public void postClose(SolrCore core) { }
    });
  }
}
