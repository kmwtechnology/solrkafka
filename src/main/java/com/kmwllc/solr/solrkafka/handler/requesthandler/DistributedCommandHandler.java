package com.kmwllc.solr.solrkafka.handler.requesthandler;

import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.MultiMapSolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.JavaBinCodec;
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
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.SolrCoreState;
import org.apache.solr.update.processor.RunUpdateProcessorFactory;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.util.plugin.SolrCoreAware;

import java.util.HashMap;
import java.util.Map;

/**
 * A {@link RequestHandlerBase} for indexing {@link AddUpdateCommand} documents sent by the
 * {@link com.kmwllc.solr.solrkafka.handler.request.CustomCommandDistributor} into a shard/replica.
 */
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
          byte[] bytes = stream.getStream().readAllBytes();
          try (JavaBinCodec codec = new JavaBinCodec()) {
            SolrInputDocument doc = (SolrInputDocument) codec.unmarshal(bytes);
            AddUpdateCommand cmd = new AddUpdateCommand(new LocalSolrQueryRequest(req.getCore(),
                new MultiMapSolrParams(Map.of())));
            cmd.solrDoc = doc;
            cmd.setIndexedId(new BytesRef(req.getParams().get("docId")));
            req.getCore().getUpdateHandler().addDoc(cmd);
          }
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