package com.kmwllc.solr.solrkafka.handlers.requesthandlers;

import com.kmwllc.solr.solrkafka.importers.Importer;
import com.kmwllc.solr.solrkafka.importers.SolrDocumentImportHandler;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

import java.util.ArrayList;

/**
 * A request handler for displaying the current status of the {@link SolrDocumentImportHandler}.
 */
public class SolrKafkaStopRequestHandler extends RequestHandlerBase {
  private static Importer handler;

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    ResponseBuilder rb = new ResponseBuilder(req, rsp, new ArrayList<>());

    // TODO: pause, resume, restart from beginning
    if (handler == null || !handler.isThreadAlive()) {
      rsp.add("Status", "SolrKafka not running");
    } else {
      handler.stop();
      rsp.add("Status", "Stopping SolrKafka");
    }
  }

  @Override
  public String getDescription() {
    return "Request handler base";
  }

  public static void setHandler(Importer handler) {
    SolrKafkaStopRequestHandler.handler = handler;
  }
}
