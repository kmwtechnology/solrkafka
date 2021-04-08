package com.kmwllc.solr.solrkafka.handler.requesthandler;

import com.kmwllc.solr.solrkafka.importer.Importer;
import com.kmwllc.solr.solrkafka.importer.SolrDocumentImportHandler;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

import java.util.ArrayList;
import java.util.Map;

/**
 * A request handler for displaying the current status of the {@link SolrDocumentImportHandler}.
 */
public class SolrKafkaStatusRequestHandler extends RequestHandlerBase {
  private static Importer handler;

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    ResponseBuilder rb = new ResponseBuilder(req, rsp, new ArrayList<>());

    rsp.add("Status",
        "SolrKafka status is " + (handler == null ? "NOT_INITIALIZED" : handler.getStatus()));
    if (handler != null && handler.isThreadAlive()) {
      Map<String, Long> consumerGroupLag = handler.getConsumerGroupLag();
      rsp.add("ConsumerGroupLag", consumerGroupLag);
    }
  }

  @Override
  public String getDescription() {
    return "Request handler base";
  }

  public static void setHandler(Importer handler) {
    SolrKafkaStatusRequestHandler.handler = handler;
  }
}