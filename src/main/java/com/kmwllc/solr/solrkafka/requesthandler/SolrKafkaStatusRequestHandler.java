package com.kmwllc.solr.solrkafka.requesthandler;

import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

import java.util.ArrayList;

public class SolrKafkaStatusRequestHandler extends RequestHandlerBase {
  private static SolrKafkaRequestHandler handler;

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    ResponseBuilder rb = new ResponseBuilder(req, rsp, new ArrayList<>());

    rsp.add("Status",
        "SolrKafka is " + (handler != null && handler.isThreadAlive() ? "" : "not ") + "running");
  }

  @Override
  public String getDescription() {
    return "Request handler base";
  }

  public static void setHandler(SolrKafkaRequestHandler handler) {
    SolrKafkaStatusRequestHandler.handler = handler;
  }
}
