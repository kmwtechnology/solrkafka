package com.kmwllc.solr.solrkafka.handlers.requesthandlers;

import com.kmwllc.solr.solrkafka.importers.Importer;
import com.kmwllc.solr.solrkafka.importers.KafkaImporter;
import com.kmwllc.solr.solrkafka.importers.SolrDocumentImportHandler;
import com.kmwllc.solr.solrkafka.handlers.consumerhandlers.AsyncKafkaConsumerHandler;
import com.kmwllc.solr.solrkafka.handlers.consumerhandlers.KafkaConsumerHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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

  public SolrKafkaRequestHandler() {
    log.info("Kafka Consumer created.");
  }
  // TODO: support deletes, updates, ... at some point

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

    Object actionObj = req.getParams().get("action");
    String action;
    if (actionObj == null) {
      action = "start";
    } else {
      action = actionObj.toString();
    }

    if (action.equalsIgnoreCase("start")) {
      if (importer != null && importer.isThreadAlive()) {
        rsp.add("Status", "Request already running");
        return;
      }

      boolean fromBeginning = req.getParams().getBool("fromBeginning", false);
      boolean readFullyAndExit = req.getParams().getBool("exitAtEnd", false);

      if (importer != null) {
        importer.stop();
      }
      if (!consumerType.equalsIgnoreCase("simple")) {
        KafkaConsumerHandler consumerHandler = KafkaConsumerHandler.getInstance(consumerType,
            initProps, topic, fromBeginning, readFullyAndExit, incomingDataType);
        importer = new SolrDocumentImportHandler(core, consumerHandler, commitInterval);
      } else {
        importer = new KafkaImporter(core, readFullyAndExit, fromBeginning, commitInterval);
      }

      SolrKafkaStatusRequestHandler.setHandler(importer);
      SolrKafkaStopRequestHandler.setHandler(importer);
      importer.startThread();
      rsp.add("Status", "Started");
    } else if (action.equalsIgnoreCase("stop")) {
      if (importer == null || !importer.isThreadAlive()) {
        rsp.add("Status", "SolrKafka not running");
      } else {
        importer.stop();
        rsp.add("Status", "Stopping SolrKafka");
      }
    } else if (action.equalsIgnoreCase("pause")) {

    }
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

    // TODO: determine if leader, only add documents if this is leader (probably)

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

  @Override
  public void inform(SolrCore core) {
    // TODO: can this get updated in the middle of a request (can get called a few times, when reload?)
    this.core = core;
    core.addCloseHook(new CloseHook() {
      @Override
      public void preClose(SolrCore core) {
        importer.stop();
      }

      @Override
      public void postClose(SolrCore core) { }
    });
  }
}
