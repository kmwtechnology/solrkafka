package com.kmwllc.solr.solrkafka.requesthandler;

import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

import java.util.ArrayList;

/**
 * A request handler for displaying the current status of the {@link SolrDocumentImportHandler}.
 */
public class SolrKafkaStopRequestHandler extends RequestHandlerBase {
  private static SolrDocumentImportHandler handler;

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    ResponseBuilder rb = new ResponseBuilder(req, rsp, new ArrayList<>());

    handler.close();
    rsp.add("Status", "Stopping SolrKafka");
  }

  @Override
  public String getDescription() {
    return "Request handler base";
  }

  public static void setHandler(SolrDocumentImportHandler handler) {
    SolrKafkaStopRequestHandler.handler = handler;
  }
}
