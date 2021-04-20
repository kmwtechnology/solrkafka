package com.kmwllc.solr.solrkafka.importer;

import com.kmwllc.solr.solrkafka.datatype.SerdeFactory;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.MultiMapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.processor.DistributedUpdateProcessorFactory;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessorChain;
import org.apache.solr.update.processor.UpdateRequestProcessorFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.TemporalAmount;
import java.util.*;

/**
 * Imports documents from a Kafka {@link Consumer} in a separate thread and adds them to Solr using the same
 * functionality as the (soon-to-be-deprecated) Document Import Handler.
 */
public class KafkaImporter implements Runnable {
  private static final Logger log = LogManager.getLogger(KafkaImporter.class);
  private volatile SolrCore core;
  private volatile UpdateRequestProcessor updateHandler;
  private final Duration pollTimeout = Duration.ofMillis(1000);
  private static final NamedList<String> SOLR_REQUEST_ARGS = new NamedList<>();
  private final Thread thread;
  private volatile boolean running = false;
  private final TemporalAmount commitInterval;
  private final Map<String, Long> consumerGroupLag = new HashMap<>();
  private final String topicName;
  private final String kafkaBroker;
  private final String dataType;

  static {
    SOLR_REQUEST_ARGS.add("commitWithin", "1000");
    SOLR_REQUEST_ARGS.add("overwrite", "true");
    SOLR_REQUEST_ARGS.add("wt", "json");
  }

  /**
   * @param core The {@link SolrCore} to add documents to
   * @param commitInterval The minimum number of MS between each Kafka offset commit
   * @param ignoreShardRouting {@code true} if all documents should be added to every shard
   */
  public KafkaImporter(SolrCore core, String kafkaBroker, String topicName, long commitInterval,
                       boolean ignoreShardRouting, String dataType) {
    this.topicName = topicName;
    this.core = core;
    this.kafkaBroker = kafkaBroker;
    this.updateHandler = createUpdateHandler(core, ignoreShardRouting);
    this.commitInterval = Duration.ofMillis(commitInterval);
    this.dataType = dataType;
    thread = new Thread(this, "KafkaImporter Async Runnable");
  }

  /**
   * Creates a {@link UpdateRequestProcessor} based off of the one provided by the {@link SolrCore}. If
   * ignoreShardRouting is {@code true}, then edit the chain provided by the core and insert our own
   * {@link AllShardUpdateProcessor} in place of the {@link AllShardUpdateProcessor}.
   *
   * @return A {@link UpdateRequestProcessor} instance
   */
  private static UpdateRequestProcessor createUpdateHandler(SolrCore core, boolean ignoreShardRouting) {
    SolrParams params = new MultiMapSolrParams(Map.of());
    UpdateRequestProcessorChain chain = core.getUpdateProcessorChain(params);
    if (!ignoreShardRouting) {
      return chain.createProcessor(new LocalSolrQueryRequest(core, params), null);
    }

    List<UpdateRequestProcessorFactory> factories = new ArrayList<>(chain.getProcessors());
    for (int i = 0; i < factories.size(); i++) {
      UpdateRequestProcessorFactory factory = factories.get(i);
      // TODO: document that we're not handling legacy master-slave mode
      if (factory instanceof DistributedUpdateProcessorFactory) {
        factory = new AllShardUpdateProcessor.AllShardUpdateProcessorFactory();
        factories.set(i, factory);
        break;
      }
    }

    return new UpdateRequestProcessorChain(factories, core)
        .createProcessor(new LocalSolrQueryRequest(core, params), null);
  }

  public void startThread() {
    log.info("Starting thread");
    running = true;
    thread.start();
  }

  public void stop() {
    log.info("Stopping importer");
    running = false;

    // Let the importer try to stop on its own
    try {
      Thread.sleep(pollTimeout.toMillis() * 5);
    } catch (InterruptedException e) {
      log.error("Thread interrupted while stopping", e);
    }

    // Interrupt the thread if it couldn't shut down on its own
    if (thread != null && thread.isAlive()) {
      log.warn("Thread took too long to shut down; interrupting thread");
      thread.interrupt();
    }
  }

  public boolean isRunning() {
    return running;
  }

  public boolean isThreadAlive() {
    return thread != null && thread.isAlive();
  }

  @Override
  public void run() {
    log.info("Starting Kafka consumer");

    long startTime = System.currentTimeMillis();
    long docCount = 0;
    long docCommitInterval = 0;
    Instant prevCommit = Instant.now();
    try (Consumer<String, SolrDocument> consumer = createConsumer()){
      while (running) {
        // Consume records from Kafka
        ConsumerRecords<String, SolrDocument> consumerRecords = consumer.poll(pollTimeout);
        if (consumerRecords.count() > 0) {
          log.info("Processing consumer records. {}", consumerRecords.count());

          // Process each record provided
          for (ConsumerRecord<String, SolrDocument> record : consumerRecords) {
            log.debug("Record received: {}", record);

            // Construct Solr create request
            SolrQueryRequest request = new LocalSolrQueryRequest(core, SOLR_REQUEST_ARGS);
            request.setJSON(record.value());
            AddUpdateCommand add = new AddUpdateCommand(request);
            add.solrDoc = convertToInputDoc(record.value());

            // Attempt to add the update
            try {
              updateHandler.processAdd(add);
              docCount++;
              docCommitInterval++;
            } catch (IOException | SolrException e) {
              log.error("Couldn't add solr doc...", e);
            }
          }

        } else {
          log.info("No records received");
        }

        // If commitInterval has elapsed, commit back to Kafka
        if (prevCommit.plus(commitInterval).isBefore(Instant.now())) {
          double interval = System.currentTimeMillis() - startTime;
          log.info("\nAverage doc processing time: {}\nTotal elapsed time: {}\nTotal docs processed: {}\nDocs processed in commit interval: {}",
              interval / docCount, interval, docCount, docCommitInterval);
          commit(consumer);
          prevCommit = Instant.now();
          docCommitInterval = 0;
        }
      }
      commit(consumer);
    } catch (Throwable e) {
      log.error("Error encountered while running importer", e);
    }

    log.info("KafkaImporter finished");
  }

  /**
   * Commits most recent offsets to Kafka asynchronously and updates consumer lag.
   */
  private void commit(Consumer<String, SolrDocument> consumer) {
    log.info("Committing back to Kafka after {} delay and updating consumer group lag info", commitInterval);
    try {
      consumer.commitAsync();
    } catch (CommitFailedException e) {
      log.error("Commit failed", e);
      consumerGroupLag.clear();
    }

    Map<TopicPartition, Long> ends = consumer.endOffsets(consumer.assignment());
    Map<TopicPartition, OffsetAndMetadata> offsets = consumer.committed(consumer.assignment());
    for (Map.Entry<TopicPartition, Long> entry : ends.entrySet()) {
      consumerGroupLag.put(entry.getKey().toString(), entry.getValue() - offsets.get(entry.getKey()).offset());
    }
  }

  public Map<String, Long> getConsumerGroupLag() {
    return consumerGroupLag;
  }

  /**
   * Creates a new Kafka {@link Consumer}, setting required values if not already provided.
   *
   * @return An initialized {@link Consumer}
   */
  private Consumer<String, SolrDocument> createConsumer() {
    Properties props = new Properties();
    props.putIfAbsent(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker);
    props.putIfAbsent(ConsumerConfig.GROUP_ID_CONFIG, core.getName());
    props.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SerdeFactory.getDeserializer(dataType).getName());
    // TODO: configurable from solrconfig or path?
    props.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//    props.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    props.putIfAbsent(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 16000);

    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(null);
    KafkaConsumer<String, SolrDocument> consumer = new KafkaConsumer<>(props);
    Thread.currentThread().setContextClassLoader(loader);

    consumer.subscribe(Collections.singletonList(topicName));
    return consumer;
  }

  public static SolrInputDocument convertToInputDoc(Map<String, Object> doc) {
    SolrInputDocument inDoc = new SolrInputDocument();

    for (String name : doc.keySet()) {
      inDoc.addField(name, doc.get(name));
    }

    return inDoc;
  }

}
