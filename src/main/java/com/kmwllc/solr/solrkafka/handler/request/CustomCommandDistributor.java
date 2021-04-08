package com.kmwllc.solr.solrkafka.handler.request;

import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.propagation.Format;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.update.processor.UpdateRequestProcessorChain;
import org.slf4j.Logger;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.SolrCmdDistributor;
import org.apache.solr.update.StreamingSolrClients;
import org.apache.solr.update.UpdateShardHandler;
import org.apache.solr.update.processor.DistributedUpdateProcessor;
import org.apache.solr.util.tracing.GlobalTracer;
import org.apache.solr.util.tracing.SolrRequestCarrier;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;

public class CustomCommandDistributor implements AutoCloseable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final StreamingSolrClients clients;
  private final CompletionService<Object> completionService;
  private final Set<Future<Object>> pending = new HashSet<>();
  private final List<SolrCmdDistributor.Error> errors = Collections.synchronizedList(new ArrayList<>());
  private final List<SolrCmdDistributor.Error> allErrors = new ArrayList<>();
  private static final int retryPause = 500;

  public CustomCommandDistributor(UpdateShardHandler updateShardHandler) {
    this.clients = new StreamingSolrClients(updateShardHandler);
    this.completionService = new ExecutorCompletionService<>(updateShardHandler.getUpdateExecutor());
  }

  @Override
  public void close() {
    clients.shutdown();
  }

  public void finish() {
    try {
      blockAndDoRetries();
    } catch (IOException e) {
      log.warn("Unable to finish sending updates", e);
    } finally {
      clients.shutdown();
    }
  }

  public void distribAdd(AddUpdateCommand cmd, List<SolrCmdDistributor.Node> nodes, ModifiableSolrParams params) throws IOException {
    distribAdd(cmd, nodes, params, false, null, null);
  }

  public void distribAdd(AddUpdateCommand cmd, List<SolrCmdDistributor.Node> nodes, ModifiableSolrParams params, boolean synchronous) throws IOException {
    distribAdd(cmd, nodes, params, synchronous, null, null);
  }

  public void distribAdd(AddUpdateCommand cmd, List<SolrCmdDistributor.Node> nodes, ModifiableSolrParams params, boolean synchronous,
                         DistributedUpdateProcessor.RollupRequestReplicationTracker rollupTracker,
                         DistributedUpdateProcessor.LeaderRequestReplicationTracker leaderTracker) throws IOException {
    for (SolrCmdDistributor.Node node : nodes) {
      UpdateRequest uReq = new UpdateRequest("/kafka/distrib");
      if (cmd.isLastDocInBatch)
        uReq.lastDocInBatch();
      uReq.setParams(params);
      uReq.add(cmd.solrDoc, cmd.commitWithin, cmd.overwrite);
      if (cmd.isInPlaceUpdate()) {
        params.set(DistributedUpdateProcessor.DISTRIB_INPLACE_PREVVERSION, String.valueOf(cmd.prevVersion));
      }
      submit(new SolrCmdDistributor.Req(cmd, node, uReq, synchronous, rollupTracker, leaderTracker));
    }

  }

  private void submit(final SolrCmdDistributor.Req req) throws IOException {
    // Copy user principal from the original request to the new update request, for later authentication interceptor use
    if (SolrRequestInfo.getRequestInfo() != null) {
      req.uReq.setUserPrincipal(SolrRequestInfo.getRequestInfo().getReq().getUserPrincipal());
    }

    Tracer tracer = GlobalTracer.getTracer();
    Span parentSpan = tracer.activeSpan();
    if (parentSpan != null) {
      tracer.inject(parentSpan.context(), Format.Builtin.HTTP_HEADERS,
          new SolrRequestCarrier(req.uReq));
    }

    if (req.synchronous) {
      blockAndDoRetries();

      try {
        req.uReq.setBasePath(req.node.getUrl());
        clients.getHttpClient().request(req.uReq);
      } catch (Exception e) {
        SolrException.log(log, e);
        SolrCmdDistributor.Error error = new SolrCmdDistributor.Error();
        error.e = e;
        error.req = req;
        if (e instanceof SolrException) {
          error.statusCode = ((SolrException) e).code();
        }
        errors.add(error);
      }

      return;
    }

    if (log.isDebugEnabled()) {
      log.debug("sending update to {} retry: {} {} params {}"
          , req.node.getUrl(), req.retries, req.cmd, req.uReq.getParams());
    }

    doRequest(req);
  }

  private void doRequest(final SolrCmdDistributor.Req req) {
    try {
      SolrClient solrClient = clients.getSolrClient(req);
      solrClient.request(req.uReq);
    } catch (Exception e) {
      SolrException.log(log, e);
      SolrCmdDistributor.Error error = new SolrCmdDistributor.Error();
      error.e = e;
      error.req = req;
      if (e instanceof SolrException) {
        error.statusCode = ((SolrException) e).code();
      }
      errors.add(error);
    }
  }

  public void blockAndDoRetries() throws IOException {
    clients.blockUntilFinished();

    // wait for any async commits to complete
    while (pending != null && pending.size() > 0) {
      Future<Object> future = null;
      try {
        future = completionService.take();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        log.error("blockAndDoRetries interrupted", e);
      }
      if (future == null) break;
      pending.remove(future);
    }
    doRetriesIfNeeded();

  }

  private void doRetriesIfNeeded() throws IOException {
    // NOTE: retries will be forwards to a single url

    List<SolrCmdDistributor.Error> errors = new ArrayList<>(this.errors);
    errors.addAll(clients.getErrors());
    List<SolrCmdDistributor.Error> resubmitList = new ArrayList<>();

    if (log.isInfoEnabled() && errors.size() > 0) {
      log.info("SolrCmdDistributor found {} errors", errors.size());
    }

    if (log.isDebugEnabled() && errors.size() > 0) {
      StringBuilder builder = new StringBuilder("SolrCmdDistributor found:");
      int maxErrorsToShow = 10;
      for (SolrCmdDistributor.Error e:errors) {
        if (maxErrorsToShow-- <= 0) break;
        builder.append("\n").append(e);
      }
      if (errors.size() > 10) {
        builder.append("\n... and ");
        builder.append(errors.size() - 10);
        builder.append(" more");
      }
      log.debug("{}", builder);
    }

    for (SolrCmdDistributor.Error err : errors) {
      try {
        /*
         * if this is a retryable request we may want to retry, depending on the error we received and
         * the number of times we have already retried
         */
        boolean isRetry = err.req.shouldRetry(err);

        // this can happen in certain situations such as close
        if (isRetry) {
          err.req.retries++;
          resubmitList.add(err);
        } else {
          allErrors.add(err);
        }
      } catch (Exception e) {
        // continue on
        log.error("Unexpected Error while doing request retries", e);
      }
    }

    if (resubmitList.size() > 0) {
      // Only backoff once for the full batch
      try {
        int backoffTime = Math.min(retryPause * resubmitList.get(0).req.retries, 2000);
        if (log.isDebugEnabled()) {
          log.debug("Sleeping {}ms before re-submitting {} requests", backoffTime, resubmitList.size());
        }
        Thread.sleep(backoffTime);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        log.warn(null, e);
      }
    }

    clients.clearErrors();
    this.errors.clear();
    for (SolrCmdDistributor.Error err : resubmitList) {
      if (err.req.node instanceof SolrCmdDistributor.ForwardNode) {
        SolrException.log(log, "forwarding update to "
            + err.req.node.getUrl() + " failed - retrying ... retries: "
            + err.req.retries + "/" + err.req.node.getMaxRetries() + ". "
            + err.req.cmd.toString() + " params:"
            + err.req.uReq.getParams() + " rsp:" + err.statusCode, err.e);
      } else {
        SolrException.log(log, "FROMLEADER request to "
            + err.req.node.getUrl() + " failed - retrying ... retries: "
            + err.req.retries + "/" + err.req.node.getMaxRetries() + ". "
            + err.req.cmd.toString() + " params:"
            + err.req.uReq.getParams() + " rsp:" + err.statusCode, err.e);
      }
      submit(err.req);
    }

    if (resubmitList.size() > 0) {
      blockAndDoRetries();
    }
  }
}
