package com.kmwllc.solr.solrkafka.requesthandler;

import com.kmwllc.solr.solrkafka.requesthandler.consumerhandlers.KafkaConsumerHandler;
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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Semaphore;

/**
 * Handles inserting documents returned by the {@link KafkaConsumerHandler} into Solr.
 */
public class SolrDocumentImportHandler implements Runnable, AutoCloseable {
  private static final Logger log = LogManager.getLogger(SolrDocumentImportHandler.class);

  private final SolrCore core;
  private final UpdateHandler updateHandler;
  private Thread thread;
  private KafkaConsumerHandler consumerHandler;
  // TODO: would this be better as an atoimc reference?
  private final Semaphore mapLock = new Semaphore(1);
  private final Map<TopicPartition, OffsetAndMetadata> addedOffsets = new HashMap<>();

  public SolrDocumentImportHandler(SolrCore core, KafkaConsumerHandler consumerHandler) {
    this.core = core;
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
    if (consumerHandler.hasAlreadyRun()) {
      throw new IllegalStateException("Consumer handler has not been (re-)initialized");
    }
    thread = new Thread(this);
    thread.start();
  }

  /**
   * Sets the {@link UpdateHandler}'s callback. Used for committing offsets when the Solr index is committed.
   */
  private void setUpdateHandlerCallback() {
    updateHandler.registerCommitCallback(new SolrEventListener() {
      @Override
      public void postCommit() {
        try {
          mapLock.acquire();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }

        if (!addedOffsets.isEmpty()) {
          consumerHandler.commitOffsets(addedOffsets);
          addedOffsets.clear();
        }
        if (!consumerHandler.isRunning()) {
          consumerHandler.stop();
        }
        mapLock.release();
        log.info("Updated offsets");
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
  public void close() {
    consumerHandler.stop();
    if (thread != null && thread.isAlive()) {
      thread.interrupt();
    }
  }

  /**
   * Processes documents provided by the {@link KafkaConsumerHandler} and provides them to the {@link UpdateHandler}.
   * Keeps track of committed indices for topics and provides them to {@link KafkaConsumerHandler#commitOffsets(Map)}
   * when the Solr index is committed.
   */
  @Override
  public void run() {
    log.info("Starting Kafka consumer");

    while (consumerHandler.hasNext()) {
      DocumentData doc = consumerHandler.next();
      AddUpdateCommand add = new AddUpdateCommand(buildReq(doc.getDoc()));
      add.solrDoc = doc.convertToInputDoc();
      try {
        updateHandler.addDoc(add);
        try {
          mapLock.acquire();
        } catch (InterruptedException e) {
          thread.interrupt();
        }
        addedOffsets.put(doc.getPart(), doc.getOffset());
        mapLock.release();
      } catch (IOException | SolrException e) {
        log.error("Error occurred with Kafka index handler", e);
        return;
      }
    }

    log.info("Kafka consumer finished");
  }

  /**
   * Builds a {@link SolrQueryRequest} to be passed to {@link UpdateHandler#addDoc(AddUpdateCommand)}.
   * 
   * @param doc The document to be added to Solr
   * @return A prepared request
   */
  private SolrQueryRequest buildReq(Map<String, Object> doc) {
    @SuppressWarnings("unchecked")
    Map.Entry<String, String>[] nl = new NamedList.NamedListEntry[3];
    nl[0] = new NamedList.NamedListEntry<>("commitWithin", "1000");
    nl[1] = new NamedList.NamedListEntry<>("overwrite", "true");
    nl[2] = new NamedList.NamedListEntry<>("wt", "json");
    SolrQueryRequest req = new LocalSolrQueryRequest(core, new NamedList<>(nl));
    req.setJSON(doc);
    return req;
  }

  public boolean isThreadAlive() {
    return thread != null && thread.isAlive();
  }
}
