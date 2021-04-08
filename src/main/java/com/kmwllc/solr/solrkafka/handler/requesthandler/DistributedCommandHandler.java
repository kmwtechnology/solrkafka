package com.kmwllc.solr.solrkafka.handler.requesthandler;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.ContentStreamHandlerBase;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.handler.RequestHandlerUtils;
import org.apache.solr.handler.UpdateRequestHandler;
import org.apache.solr.handler.loader.CSVLoader;
import org.apache.solr.handler.loader.ContentStreamLoader;
import org.apache.solr.handler.loader.JavabinLoader;
import org.apache.solr.handler.loader.JsonLoader;
import org.apache.solr.handler.loader.XMLLoader;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.SolrCoreState;
import org.apache.solr.update.processor.RunUpdateProcessorFactory;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.util.plugin.SolrCoreAware;

import java.util.HashMap;
import java.util.Map;

public class DistributedCommandHandler extends RequestHandlerBase {

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    SolrCoreState solrCoreState = req.getCore().getSolrCoreState();
    if (!solrCoreState.registerInFlightUpdate())  {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Updates are temporarily paused for core: " + req.getCore().getName());
    }
    try {
      Iterable<ContentStream> streams = req.getContentStreams();
      if (streams != null) {
        for (ContentStream stream : streams) {
          ContentStreamLoader loader = new JavabinLoader();
          loader.load(req, rsp, stream, new RunUpdateProcessorFactory().getInstance(req, rsp, null));
        }
      }
    } finally {
      solrCoreState.deregisterInFlightUpdate();
    }
  }

  @Override
  public String getDescription() {
    return "Handles adding document copies to shards";
  }
}