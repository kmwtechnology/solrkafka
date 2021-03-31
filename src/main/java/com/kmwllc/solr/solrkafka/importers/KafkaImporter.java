package com.kmwllc.solr.solrkafka.importers;

import com.kmwllc.solr.solrkafka.datatypes.DocumentData;
import com.kmwllc.solr.solrkafka.datatypes.solr.SolrDocumentDeserializer;
import org.apache.kafka.clients.consumer.*;
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
import java.util.Properties;
import java.util.concurrent.Semaphore;

public class KafkaImporter implements Runnable, Importer {

  private static final Logger log = LogManager.getLogger(KafkaImporter.class);

  private final SolrCore core;

  private final UpdateHandler updateHandler;

  private final Consumer<String, SolrDocument> consumer;

  private final Duration pollTimeout = Duration.ofMillis(1000);

  private static final NamedList SOLR_REQUEST_ARGS = new NamedList();

  private final Thread thread;

  private volatile boolean running = false;

  private final boolean readFullyAndExit;

  private volatile boolean isClosed = false;

  private final TemporalAmount commitInterval;

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
    this.consumer = createConsumer(fromBeginning);
    setUpdateHandlerCallback();
    this.thread = new Thread(this);
  }

  /**
   * Sets the {@link UpdateHandler}'s callback. Used for committing offsets when the Solr index is committed.
   */
  private void setUpdateHandlerCallback() {
    updateHandler.registerCommitCallback(new SolrEventListener() {
      @Override
      public void postCommit() {
        if (isClosed) {
          return;
        }
        if (!running && thread.isAlive()) {
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
    running = true;
    thread.start();
  }

  @Override
  public void stop() {
    running = false;
    try {
      Thread.sleep(pollTimeout.toMillis() * 2);
    } catch (InterruptedException ignored) {
    }
    consumer.close();
    isClosed = true;
    thread.interrupt();
  }

  @Override
  public boolean isThreadAlive() {
    return thread != null && thread.isAlive();
  }

  @Override
  public void run() {
    log.info("Starting Kafka consumer");

    Instant prevCommit = Instant.now();
    while (running) {
      ConsumerRecords<String, SolrDocument> consumerRecords = consumer.poll(pollTimeout);
      if (consumerRecords.count() > 0) {
        log.info("Processing consumer records. {}", consumerRecords.count());
        for (ConsumerRecord<String, SolrDocument> record : consumerRecords) {
          SolrQueryRequest request = new LocalSolrQueryRequest(core, SOLR_REQUEST_ARGS);
          request.setJSON(record.value());
          AddUpdateCommand add = new AddUpdateCommand(request);
          add.solrDoc = DocumentData.convertToInputDoc(record.value());
          try {
            updateHandler.addDoc(add);
          } catch (IOException e) {
            log.error("Couldn't add solr doc...", e);
          }
        }

        if (prevCommit.plus(commitInterval).isBefore(Instant.now())) {
          consumer.commitAsync();
          prevCommit = Instant.now();
        }
      } else {
        // TODO: remove once stop() is working
        if (readFullyAndExit) {
          running = false;
        }
      }
    }
    consumer.commitAsync();
    consumer.close();
    isClosed = true;
  }

  private static Consumer<String, SolrDocument> createConsumer(boolean fromBeginning) {
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
    if (fromBeginning) {
      consumer.poll(0);
      consumer.seekToBeginning(consumer.assignment());
    }
    return consumer;
  }
}
