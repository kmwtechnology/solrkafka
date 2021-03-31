package com.kmwllc.solr.solrkafka.handlers.requesthandlers;

import com.kmwllc.solr.solrkafka.importers.Importer;
import com.kmwllc.solr.solrkafka.importers.SolrDocumentImportHandler;
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

    // TODO: get consumer group lag per partition (max offset of partition and current offset of consumer)
    rsp.add("Status",
        "SolrKafka is " + (handler != null && handler.isThreadAlive() ? "" : "not ") + "running");
    Map<String, Long> consumerGroupLag = null;
    if (handler != null && handler.isThreadAlive()) {
      consumerGroupLag = handler.getConsumerGroupLag();
    }
    rsp.add("ConsumerGroupLag", consumerGroupLag);
  }

  @Override
  public String getDescription() {
    return "Request handler base";
  }

  public static void setHandler(Importer handler) {
    SolrKafkaStatusRequestHandler.handler = handler;
  }
}
