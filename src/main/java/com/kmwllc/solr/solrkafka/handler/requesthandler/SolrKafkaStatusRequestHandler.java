package com.kmwllc.solr.solrkafka.handler.requesthandler;

import com.kmwllc.solr.solrkafka.importer.KafkaImporter;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

import java.util.ArrayList;
import java.util.Map;

/**
 * A request handler for displaying the current status of the kafka importer.
 */
public class SolrKafkaStatusRequestHandler extends RequestHandlerBase {
  private static KafkaImporter handler;

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    ResponseBuilder rb = new ResponseBuilder(req, rsp, new ArrayList<>());

  }

  @Override
  public String getDescription() {
    return "Request handler base";
  }

  public static void setHandler(KafkaImporter handler) {
    SolrKafkaStatusRequestHandler.handler = handler;
  }
}
