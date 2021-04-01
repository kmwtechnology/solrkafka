package com.kmwllc.solr.solrkafka.importers;

import com.kmwllc.solr.solrkafka.datatypes.DocumentData;
import com.kmwllc.solr.solrkafka.datatypes.solr.SolrDocumentDeserializer;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.common.SolrDocument;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Semaphore;

public class KafkaImporter implements Runnable, Importer {

  private static final Logger log = LogManager.getLogger(KafkaImporter.class);

  private volatile SolrCore core;

  private volatile UpdateHandler updateHandler;

  private final Consumer<String, SolrDocument> consumer;

  private final Duration pollTimeout = Duration.ofMillis(1000);

  private static final NamedList SOLR_REQUEST_ARGS = new NamedList();

  private final Thread thread;

  private volatile Status status = Status.NOT_STARTED;

  private final boolean readFullyAndExit;

  private volatile boolean isClosed = false;

  private final TemporalAmount commitInterval;

  private volatile boolean rewind;

  private final Map<String, Long> consumerGroupLag = new HashMap<>();

  static {
    SOLR_REQUEST_ARGS.add("commitWithin", "1000");
    SOLR_REQUEST_ARGS.add("overwrite", "true");
    SOLR_REQUEST_ARGS.add("wt", "json");
  }

  public KafkaImporter(SolrCore core, boolean readFullyAndExit, boolean fromBeginning, long commitInterval) {
    this.core = core;
    this.updateHandler = core.getUpdateHandler();
    this.readFullyAndExit = readFullyAndExit;
    this.commitInterval = Duration.ofMillis(commitInterval);
    rewind = fromBeginning;
    setUpdateHandlerCallback();
    this.consumer = createConsumer();
    thread = new Thread(this);
  }

  /**
   * Sets the {@link UpdateHandler}'s callback. Used for committing offsets when the Solr index is committed.
   */
  private void setUpdateHandlerCallback() {
    log.info("Registering UpdateHandler callback");
    updateHandler.registerCommitCallback(new SolrEventListener() {
      @Override
      public void postCommit() {
        log.debug("UpdateHandler postCommit callback");
        if (isClosed) {
          return;
        }
        if (!status.isOperational() && thread != null && thread.isAlive()) {
          log.info("Shutting down Importer");
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

  @Override
  public void startThread() {
    log.info("Starting thread");
    status = Status.RUNNING;
    thread.start();
  }

  @Override
  public void stop() {
    log.info("Stopping importer");
    status = Status.DONE;
    try {
      Thread.sleep(pollTimeout.toMillis() * 5);
    } catch (InterruptedException ignored) {
    }
    if (thread != null && thread.isAlive()) {
      log.warn("Thread took too long to shut down; interrupting thread");
      thread.interrupt();
    }
    if (!isClosed) {
      log.warn("Consumer not closed in regular thread shutdown");
      consumer.close();
      isClosed = true;
    }
  }

  @Override
  public void pause() {
    if (status.isOperational()) {
      status = Status.PAUSED;
    }
  }

  @Override
  public void resume() {
    if (status.isOperational()){
      status = Status.RUNNING;
    }
  }

  @Override
  public void rewind() {
    rewind = true;
  }

  @Override
  public Status getStatus() {
    return status;
  }

  @Override
  public void setNewCore(SolrCore core) {
    this.core = core;
    this.updateHandler = core.getUpdateHandler();
  }

  @Override
  public boolean isThreadAlive() {
    return thread != null && thread.isAlive() || !isClosed;
  }

  @Override
  public void run() {
    log.info("Starting Kafka consumer");

    Instant prevCommit = Instant.now();
    while (status.isOperational()) {
      if (rewind) {
        log.info("Initiating rewind");
        consumer.poll(0);
        consumer.seekToBeginning(consumer.assignment());
        rewind = false;
      }

      final Status localStatus = status;

      if (localStatus == Status.PAUSED && consumer.paused().size() != consumer.assignment().size()) {
        log.info("Pausing unpaused Kafka consumer assignments");
        consumer.pause(consumer.assignment());
      } else if (localStatus == Status.RUNNING && !consumer.paused().isEmpty()) {
        log.info("Resuming paused Kafka consumer assignments");
        consumer.resume(consumer.assignment());
      }

      ConsumerRecords<String, SolrDocument> consumerRecords = consumer.poll(pollTimeout);
      if (consumerRecords.count() > 0) {
        log.info("Processing consumer records. {}", consumerRecords.count());
        for (ConsumerRecord<String, SolrDocument> record : consumerRecords) {
          log.debug("Record received: {}", record);
          SolrQueryRequest request = new LocalSolrQueryRequest(core, SOLR_REQUEST_ARGS);
          request.setJSON(record.value());
          AddUpdateCommand add = new AddUpdateCommand(request);
          add.solrDoc = DocumentData.convertToInputDoc(record.value());
          try {
            updateHandler.addDoc(add);
          } catch (IOException e) {
            log.error("Couldn't add solr doc...", e);
            status = Status.ERROR;
          }
        }

      } else {
        log.info("No records received");
        // TODO: remove once stop() is working
        if (readFullyAndExit && localStatus != Status.PAUSED) {
          status = Status.DONE;
        }
      }

      if (prevCommit.plus(commitInterval).isBefore(Instant.now())) {
        commit();
        prevCommit = Instant.now();
      }
    }

    log.info("Cleaning up thread");
    commit();
    consumer.close();
    isClosed = true;
    log.info("KafkaImporter finished");
  }

  public void commit() {
    log.info("Committing back to Kafka after {} delay and updating consumer group lag info", commitInterval);
    try {
      consumer.commitAsync();
    } catch (CommitFailedException e) {
      log.error("Commit failed", e);
      status = Status.ERROR;
    }

    Map<TopicPartition, Long> ends = consumer.endOffsets(consumer.assignment());
    Map<TopicPartition, OffsetAndMetadata> offsets = consumer.committed(consumer.assignment());
    for (Map.Entry<TopicPartition, Long> entry : ends.entrySet()) {
      consumerGroupLag.put(entry.getKey().toString(), entry.getValue() - offsets.get(entry.getKey()).offset());
    }
  }

  @Override
  public Map<String, Long> getConsumerGroupLag() {
    return consumerGroupLag;
  }

  private static Consumer<String, SolrDocument> createConsumer() {
    Properties props = new Properties();
    props.putIfAbsent(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.putIfAbsent(ConsumerConfig.GROUP_ID_CONFIG, "SolrKafkaConsumer");
    props.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SolrDocumentDeserializer.class.getName());
    props.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.putIfAbsent(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 16000);

    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(null);
    KafkaConsumer<String, SolrDocument> consumer = new KafkaConsumer<>(props);
    Thread.currentThread().setContextClassLoader(loader);

    consumer.subscribe(Collections.singletonList("testtopic"));
    return consumer;
  }
}
