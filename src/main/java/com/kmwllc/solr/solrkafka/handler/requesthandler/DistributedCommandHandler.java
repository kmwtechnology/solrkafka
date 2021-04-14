package com.kmwllc.solr.solrkafka.handler.requesthandler;

import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.MultiMapSolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.SolrCoreState;
import org.apache.solr.update.processor.DistributedUpdateProcessorFactory;
import org.apache.solr.update.processor.UpdateRequestProcessorChain;
import org.apache.solr.update.processor.UpdateRequestProcessorFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
            final List<UpdateRequestProcessorFactory> factories = new ArrayList<>(req.getCore().getUpdateProcessorChain(
                new MultiMapSolrParams(Map.of())).getProcessors()).stream()
                .filter(fac -> !(fac instanceof DistributedUpdateProcessorFactory)).collect(Collectors.toList());
            new UpdateRequestProcessorChain(factories, req.getCore()).createProcessor(req, rsp).processAdd(cmd);
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