package com.kmwllc.solr.solrkafka.importer;

import com.kmwllc.solr.solrkafka.datatype.SerdeFactory;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.OffsetSpec;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * Imports documents from a Kafka {@link Consumer} in a separate thread and adds them to Solr using the same
 * functionality as the (soon-to-be-deprecated) Document Import Handler.
 */
public class KafkaImporter implements Runnable {
  private static final Logger log = LogManager.getLogger(KafkaImporter.class);
  public static final String KAFKA_IMPORTER_GROUP = "KafkaImporterGroup";
  private final SolrCore core;
  private final UpdateRequestProcessor updateHandler;
  private final Duration pollTimeout = Duration.ofMillis(1000);
  private static final NamedList<String> SOLR_REQUEST_ARGS = new NamedList<>();
  private final Thread thread;
  private volatile boolean running = false;
  private final TemporalAmount commitInterval;
  private final List<String> topicNames;
  private final String kafkaBroker;
  private final String dataType;
  private final boolean ignoreShardRouting;
  private final int kafkaPollInterval;
  private final boolean autoOffsetResetBeginning;

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
  public KafkaImporter(SolrCore core, String kafkaBroker, List<String> topicNames, long commitInterval,
                       boolean ignoreShardRouting, String dataType, int kafkaPollInterval, boolean autoOffsetResetBeginning) {
    this.topicNames = topicNames;
    this.ignoreShardRouting = ignoreShardRouting;
    this.core = core;
    this.kafkaBroker = kafkaBroker;
    this.updateHandler = createUpdateHandler(core);
    this.commitInterval = Duration.ofMillis(commitInterval);
    this.kafkaPollInterval = kafkaPollInterval;
    this.autoOffsetResetBeginning = autoOffsetResetBeginning;
    this.dataType = dataType;
    thread = new Thread(this, "KafkaImporter Async Runnable");
  }

  /**
   * Creates a {@link UpdateRequestProcessor} based off of the one provided by the {@link SolrCore}. If
   * ignoreShardRouting is {@code true}, then edit the chain provided by the core and insert our own
   *
   * @return A {@link UpdateRequestProcessor} instance
   */
  private UpdateRequestProcessor createUpdateHandler(SolrCore core) {
    SolrParams params = new MultiMapSolrParams(Map.of());
    UpdateRequestProcessorChain chain = core.getUpdateProcessorChain(params);
    if (!ignoreShardRouting) {
      return chain.createProcessor(new LocalSolrQueryRequest(core, params), null);
    }

    List<UpdateRequestProcessorFactory> factories = new ArrayList<>(chain.getProcessors());
    return new UpdateRequestProcessorChain(
        factories.stream().filter(fac -> !(fac instanceof DistributedUpdateProcessorFactory)).collect(Collectors.toList()),
        core).createProcessor(new LocalSolrQueryRequest(core, params), null);
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
    try (Consumer<String, SolrDocument> consumer = createConsumer()) {
      while (running) {
        // Consume records from Kafka
        if (!core.getSolrCoreState().registerInFlightUpdate()) {
          running = false;
          log.info("In flight update denied, stopping importer");
          break;
        }
        try {
          ConsumerRecords<String, SolrDocument> consumerRecords = consumer.poll(pollTimeout);
          if (!consumerRecords.isEmpty()) {
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
        } finally {
          core.getSolrCoreState().deregisterInFlightUpdate();
        }

        // If commitInterval has elapsed, commit back to Kafka
        if (prevCommit.plus(commitInterval).isBefore(Instant.now())) {
          double interval = System.currentTimeMillis() - startTime;
          log.info("\nAverage doc processing time: {} ms\nTotal elapsed time: {}\nTotal docs processed: {}\nDocs processed in commit interval: {}" +
                  "\nLast Interval Length: {} seconds",
              interval / docCount, interval, docCount, docCommitInterval,
              (Instant.now().toEpochMilli() - prevCommit.toEpochMilli()) / 1000.0);
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
    }
  }

  public static Map<String, Long> getConsumerGroupLag(String kafkaBroker, List<String> topics) {
    Properties props = new Properties();
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker);
    try (Admin admin = Admin.create(props)) {
      Map<String, Long> map = new HashMap<>();
      Map<TopicPartition, Long> ends = new HashMap<>();
      Collection<ConsumerGroupListing> groups = admin.listConsumerGroups().valid().get(10000, TimeUnit.MILLISECONDS);
      for (ConsumerGroupListing group : groups) {
        if (!group.groupId().contains(KAFKA_IMPORTER_GROUP)) {
          continue;
        }
        Map<TopicPartition, OffsetAndMetadata> offsets = admin.listConsumerGroupOffsets(group.groupId())
            .partitionsToOffsetAndMetadata().get(10000, TimeUnit.MILLISECONDS);
        if (ends.isEmpty()) {
          ends.putAll(
              admin.listOffsets(
                  offsets.entrySet().stream().collect(
                      Collectors.toMap(Map.Entry::getKey, o -> OffsetSpec.latest()))).all()
                  .get(10000, TimeUnit.MILLISECONDS)
                  .entrySet().stream().filter(offset -> topics.contains(offset.getKey().topic()))
                  .collect(Collectors.toMap(Map.Entry::getKey, o -> o.getValue().offset())));
        }
        offsets.entrySet().stream().filter(val -> ends.containsKey(val.getKey()))
            .forEach(k -> map.put(k.getKey() + ":" + group.groupId(), ends.get(k.getKey()) - k.getValue().offset()));
      }
      return map;
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * Creates a new Kafka {@link Consumer}, setting required values if not already provided.
   *
   * @return An initialized {@link Consumer}
   */
  private Consumer<String, SolrDocument> createConsumer() {
    Properties props = new Properties();
    props.putIfAbsent(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker);
    final String group = KAFKA_IMPORTER_GROUP + (ignoreShardRouting ? ":" + core.getName() : "");
    props.putIfAbsent(ConsumerConfig.GROUP_ID_CONFIG, group);
    props.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SerdeFactory.getDeserializer(dataType).getName());
    props.putIfAbsent(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    props.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetResetBeginning ? "earliest" : "latest");
    props.putIfAbsent(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, kafkaPollInterval);

    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(null);
    KafkaConsumer<String, SolrDocument> consumer = new KafkaConsumer<>(props);
    Thread.currentThread().setContextClassLoader(loader);

    consumer.subscribe(topicNames);
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
