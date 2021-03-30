package com.kmwllc.solr.solrkafka.requesthandler;

import com.kmwllc.solr.solrkafka.requesthandler.consumerhandlers.AsyncKafkaConsumerHandler;
import com.kmwllc.solr.solrkafka.requesthandler.consumerhandlers.KafkaConsumerHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
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
  private SolrDocumentImportHandler importer;
  private final Properties initProps = new Properties();

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

    if (importer != null && importer.isThreadAlive()) {
      rsp.add("Status", "Request already running");
      return;
    }

    boolean fromBeginning = req.getParams().getBool("fromBeginning", false);
    boolean readFullyAndExit = req.getParams().getBool("exitAtEnd", false);
    // TODO: should the type be configured as a path param or solrconfig value?
    String consumerType = req.getParams().get("consumerType", "sync");

    if (importer != null) {
      importer.close();
      KafkaConsumerHandler consumerHandler = KafkaConsumerHandler.getInstance(consumerType,
          initProps, topic, fromBeginning, readFullyAndExit);
      importer.setConsumerHandler(consumerHandler);
    }
    else {
      KafkaConsumerHandler consumerHandler = KafkaConsumerHandler.getInstance(consumerType,
          initProps, topic, fromBeginning, readFullyAndExit);
      importer = new SolrDocumentImportHandler(core, consumerHandler);
    }
    SolrKafkaStatusRequestHandler.setHandler(importer);
    importer.startThread();
    rsp.add("Status", "Started");
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
  }

  @Override
  public void inform(SolrCore core) {
    this.core = core;
    // TODO: add core close hook
    // immediately stop kafka and make sure stuff gets committed (or just shutdown immediately)
  }
}
