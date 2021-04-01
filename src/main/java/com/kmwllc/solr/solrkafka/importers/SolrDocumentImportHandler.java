package com.kmwllc.solr.solrkafka.importers;

import com.kmwllc.solr.solrkafka.datatypes.DocumentData;
import com.kmwllc.solr.solrkafka.handlers.consumerhandlers.KafkaConsumerHandler;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrEventListener;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.UpdateHandler;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.TemporalAmount;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Semaphore;

/**
 * Handles inserting documents returned by the {@link KafkaConsumerHandler} into Solr.
 */
public class SolrDocumentImportHandler implements Runnable, Importer {
  private static final Logger log = LogManager.getLogger(SolrDocumentImportHandler.class);
  private static final NamedList SOLR_REQUEST_ARGS = new NamedList();

  private volatile SolrCore core;
  private volatile UpdateHandler updateHandler;
  private Thread thread;
  private KafkaConsumerHandler consumerHandler;
  private final Map<TopicPartition, OffsetAndMetadata> addedOffsets = new HashMap<>();
  private final TemporalAmount commitInterval;

  static {
    SOLR_REQUEST_ARGS.add("commitWithin", "1000");
    SOLR_REQUEST_ARGS.add("overwrite", "true");
    SOLR_REQUEST_ARGS.add("wt", "json");
  }

  public SolrDocumentImportHandler(SolrCore core, KafkaConsumerHandler consumerHandler, long commitInterval) {
    this.core = core;
    this.commitInterval = Duration.ofMillis(commitInterval);
    updateHandler = core.getUpdateHandler();
    setUpdateHandlerCallback();
    this.consumerHandler = consumerHandler;
  }

  /**
   * Starts processing the documents retrieved by the {@link KafkaConsumerHandler} in a separate thread.
   * Requires a new {@code KafkaConsumerHandler} to be set using {@link this#setConsumerHandler(KafkaConsumerHandler)}
   * if this is being reused.
   */
  public void startThread() {
    if (consumerHandler.getStatus() == Status.DONE || consumerHandler.getStatus() == Status.ERROR) {
      throw new IllegalStateException("Consumer handler has not been (re-)initialized");
    }
    log.info("Creating and starting thread");
    thread = new Thread(this);
    thread.start();
  }

  @Override
  public void pause() {
    consumerHandler.pause();
  }

  @Override
  public void resume() {
    consumerHandler.resume();
  }

  @Override
  public void rewind() {
    consumerHandler.rewind();
  }

  @Override
  public Status getStatus() {
    return consumerHandler.getStatus();
  }

  @Override
  public void setNewCore(SolrCore core) {
    this.core = core;
    this.updateHandler = core.getUpdateHandler();
  }

  /**
   * Sets the {@link UpdateHandler}'s callback. Used for committing offsets when the Solr index is committed.
   */
  private void setUpdateHandlerCallback() {
    updateHandler.registerCommitCallback(new SolrEventListener() {
      @Override
      public void postCommit() {
        if (!consumerHandler.isRunning()) {
          log.info("Shutting down SolrDocumentImportHandler in callback");
          stop();
        }
      }

      @Override
      public void postSoftCommit() { }

      @Override
      public void newSearcher(SolrIndexSearcher newSearcher, SolrIndexSearcher currentSearcher) { }

      @Override
      public void init(NamedList args) { }
    });

  }

  public void setConsumerHandler(KafkaConsumerHandler handler) {
    consumerHandler = handler;
  }

  /**
   * Stops the consumer handler and interrupts this runnable thread if it's still running.
   */
  @Override
  public void stop() {
    log.info("Stopping Importer");
    consumerHandler.stop();
    if (thread != null && thread.isAlive()) {
      log.info("Thread took too long to shut down; interrupting thread");
      thread.interrupt();
    }
  }

  /**
   * Processes documents provided by the {@link KafkaConsumerHandler} and provides them to the {@link UpdateHandler}.
   * Keeps track of committed indices for topics and provides them to {@link KafkaConsumerHandler#commitOffsets}
   * when the Solr index is committed.
   */
  @Override
  public void run() {
    log.info("Starting Kafka consumer");

    Instant lastCommit = Instant.now();
    // TODO: count # docs processed to make sure not double processing
    while (consumerHandler.hasNext()) {
      DocumentData doc = consumerHandler.next();
      log.debug("Record received: {}", doc.getDoc());
      AddUpdateCommand add = new AddUpdateCommand(buildReq(doc.getDoc()));
      add.solrDoc = doc.convertToInputDoc();
      try {
        updateHandler.addDoc(add);
      } catch (IOException | SolrException e) {
        log.error("Error occurred with Kafka index handler", e);
        return;
      }

      addedOffsets.put(doc.getPart(), doc.getOffset());
      if (lastCommit.plus(commitInterval).isBefore(Instant.now())) {
        log.info("Committing offsets using KafkaConsumerHandler after {} interval", commitInterval);
        consumerHandler.commitOffsets(addedOffsets);
        lastCommit = Instant.now();
      }
    }

    log.info("Committing offsets and stopping KafkaConsumerHandler");
    consumerHandler.commitOffsets(addedOffsets);
    consumerHandler.stop();
    log.info("Kafka consumer finished");
  }

  @Override
  public Map<String, Long> getConsumerGroupLag() {
    return consumerHandler.getConsumerGroupLag();
  }

  /**
   * Builds a {@link SolrQueryRequest} to be passed to {@link UpdateHandler#addDoc(AddUpdateCommand)}.
   * 
   * @param doc The document to be added to Solr
   * @return A prepared request
   */
  private SolrQueryRequest buildReq(Map<String, Object> doc) {
    SolrQueryRequest req = new LocalSolrQueryRequest(core, SOLR_REQUEST_ARGS);
    req.setJSON(doc);
    return req;
  }

  public boolean isThreadAlive() {
    return thread != null && thread.isAlive();
  }
}
