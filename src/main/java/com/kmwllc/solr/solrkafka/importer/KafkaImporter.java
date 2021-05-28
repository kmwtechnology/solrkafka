package com.kmwllc.solr.solrkafka.importer;

import com.kmwllc.solr.solrkafka.datatype.SerdeFactory;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
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
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.processor.DistributedUpdateProcessorFactory;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessorChain;
import org.apache.solr.update.processor.UpdateRequestProcessorFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.TemporalAmount;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * Imports documents from a Kafka {@link Consumer} in a separate thread and adds them to Solr using the same
 * functionality as the (soon-to-be-deprecated) Document Import Handler. Once a the importer stops running,
 * {@link this#startThread()} should not be called again. Instead, a new instance should be created.
 */
public class KafkaImporter implements Runnable {

  /** Logger. */
  private static final Logger log = LogManager.getLogger(KafkaImporter.class);

  /** Importer group prefix. */
  public static final String KAFKA_IMPORTER_GROUP = "KafkaImporterGroup";

  /** A {@link NamedList} of request args that will be used when creating new {@link LocalSolrQueryRequest}s. */
  private static final NamedList<String> SOLR_REQUEST_ARGS = new NamedList<>();

  /** The reference to the currently used {@link SolrCore}. */
  private final SolrCore core;

  /** The time that the KafkaImporter is allowed to block for before returning if no records are received. */
  private final Duration pollTimeout = Duration.ofMillis(1000);

  /** The thread that this importer is running in. */
  private final Thread thread;

  /**
   * Whether or not this should continue running. The {@link this#thread}
   * will stop running once this is set to false (after starting).
   */
  private volatile boolean running = false;

  /** The minimum time between offset commits back to Kafka. */
  private final TemporalAmount commitInterval;

  /** The list of Kafka topic names to pull from. */
  private final List<String> topicNames;

  /** The Kafka broker connection string. */
  private final String kafkaBroker;

  /** The data type that documents from the topic should be deserialized as. */
  private final String dataType;

  /** True if all shard routing should be performed. */
  private final boolean ignoreShardRouting;

  /** The maximum poll interval that Kafka should allow before evicting the consumer. */
  private final int kafkaPollInterval;

  /** The maximum number of documents to return on a {@link Consumer#poll(Duration)} call. */
  private final int kafkaMaxPollRecords;

  /**
   * True if the Kafka consumer should start polling from the earliest offset where no existing offsets are found.
   * False if it should start from the latest offset.
   */
  private final boolean autoOffsetResetBeginning;

  /**
   * @param core The {@link SolrCore} to add documents to
   * @param kafkaBroker The Kafka broker connect string
   * @param topicNames The list of topics to pull from
   * @param commitInterval The minimum number of MS between each Kafka offset commit
   * @param ignoreShardRouting {@code true} if all documents should be added to every shard (all shard routing)
   * @param dataType The data type that records in the topics will be deserialized as (see {@link SerdeFactory})
   * @param kafkaPollInterval The maximum poll interval before the Kafka consumer is evicted
   * @param autoOffsetResetBeginning true if the offset should be automatically set to earliest (latest is false)
   */
  public KafkaImporter(SolrCore core, String kafkaBroker, List<String> topicNames, long commitInterval,
                       boolean ignoreShardRouting, String dataType, int kafkaPollInterval, boolean autoOffsetResetBeginning,
                       int kafkaMaxPollRecords) {
    this.topicNames = topicNames;
    this.ignoreShardRouting = ignoreShardRouting;
    this.core = core;
    this.kafkaBroker = kafkaBroker;
    this.commitInterval = Duration.ofMillis(commitInterval);
    this.kafkaPollInterval = kafkaPollInterval;
    this.autoOffsetResetBeginning = autoOffsetResetBeginning;
    this.dataType = dataType;
    this.kafkaMaxPollRecords = kafkaMaxPollRecords;

    // Create the thread
    thread = new Thread(this, "KafkaImporter Async Runnable");
  }

  /**
   * Creates a {@link UpdateRequestProcessor} based off of the one provided by the {@link SolrCore}. If
   * ignoreShardRouting is {@code true}, then edit the chain provided by the core and remove all instances of
   * the {@link DistributedUpdateProcessorFactory}.
   *
   * @return A {@link UpdateRequestProcessor} instance
   */
  private UpdateRequestProcessor createUpdateHandler(SolrCore core, SolrQueryResponse res) {
    SolrParams params = new MultiMapSolrParams(Map.of());
    UpdateRequestProcessorChain chain = core.getUpdateProcessorChain(params);
    LocalSolrQueryRequest req = new LocalSolrQueryRequest(core, params);
    SolrCore.preDecorateResponse(req, res);
    if (!ignoreShardRouting) {
      return chain.createProcessor(req, res);
    }

    List<UpdateRequestProcessorFactory> factories = new ArrayList<>(chain.getProcessors());
    return new UpdateRequestProcessorChain(
        factories.stream().filter(fac -> !(fac instanceof DistributedUpdateProcessorFactory)).collect(Collectors.toList()),
        core).createProcessor(req, res);
  }

  /**
   * Set {@link this#running} to true and start the thread.
   */
  public void startThread() {
    log.info("Starting thread");
    running = true;
    thread.start();
  }

  /**
   * Stop the importer. Sets running to false and gives 5 x {@link this#pollTimeout} duration to stop on its own before
   * it's interrupted.
   */
  public void stop() {
    log.info("Stopping importer");
    running = false;

    long maxWait = System.currentTimeMillis() + 2000;
    // Let the importer try to stop on its own
    while (thread.isAlive() && System.currentTimeMillis() <= maxWait) {
      Thread.onSpinWait();
    }

    // Interrupt the thread if it couldn't shut down on its own
    if (thread.isAlive()) {
      log.warn("Thread took too long to shut down; interrupting thread");
      thread.interrupt();
    }
  }

  /**
   * Is this thread is allowed to run?
   *
   * @return true if the thread should be allowed to run
   */
  public boolean isRunning() {
    return running;
  }

  /**
   * Is this thread alive?
   *
   * @return true if the thread is running
   */
  public boolean isThreadAlive() {
    return thread.isAlive();
  }

  @Override
  public void run() {
    log.info("Starting Kafka consumer");

    // Metric info printed in the logs with every commit
    long startTime = System.currentTimeMillis();
    long docCount = 0;
    long docCommitInterval = 0;
    Instant prevCommit = Instant.now();

    final Map<TopicPartition, OffsetAndMetadata> currentRound = new HashMap<>();
    final Map<TopicPartition, OffsetAndMetadata> previousRound = new HashMap<>();

    // Create a consumer and begin processing records
    try (Consumer<String, SolrDocument> consumer = createConsumer()) {
      // Run while running is true
      while (running) {
        // Returns true if the core is not in the process of shutting down and prevents the core from shutting down on us
        // Returns false if the core is preparing to shut down
        if (!core.getSolrCoreState().registerInFlightUpdate()) {
          running = false;
          log.info("In flight update denied, stopping importer");
          break;
        }

        // If commitInterval has elapsed, commit back to Kafka
        // Done before pulling from Kafka and indexing so that we can be sure the core isn't shutting down before
        // doing this
        if (prevCommit.plus(commitInterval).isBefore(Instant.now())) {
          double interval = System.currentTimeMillis() - startTime;
          log.info("\nAverage doc processing time: {} ms\nTotal elapsed time: {}\nTotal docs processed: {}\nDocs processed in commit interval: {}" +
                  "\nLast Interval Length: {} seconds",
              interval / docCount, interval, docCount, docCommitInterval,
              (Instant.now().toEpochMilli() - prevCommit.toEpochMilli()) / 1000.0);
          commit(consumer, previousRound);
          previousRound.clear();
          previousRound.putAll(currentRound);
          currentRound.clear();

          // Update some metric info
          prevCommit = Instant.now();
          docCommitInterval = 0;
        }

        SolrQueryResponse res = new SolrQueryResponse();
        try (UpdateRequestProcessor updateHandler = createUpdateHandler(core, res)) {
          // Consume records from Kafka
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

              // Sets the doc that's actually indexed on the command
              add.solrDoc = convertToInputDoc(record.value());

              // Attempt to add the update
              try {
                updateHandler.processAdd(add);
                docCount++;
                docCommitInterval++;
                currentRound.put(new TopicPartition(record.topic(), record.partition()),
                    new OffsetAndMetadata(record.offset() + 1));
              } catch (IOException | SolrException e) {
                // TODO: what do we want to do with a failed doc?
                log.error("Couldn't add solr doc: {}", record.key(), e);
              }
            }
            updateHandler.finish();

            // Logs replication factor information (currently does not perform any check)
            Object rfValue = res.getResponseHeader().get("rf");
            // TODO: could check failed request status
            if (rfValue == null) {
              log.info("No RF value found: {}", res.getResponseHeader());
            } else {
              Integer rf = Integer.parseInt(rfValue.toString());
              log.info("RF found {}", rf);
            }

            if (core.getCoreContainer().isShutDown()) {
              currentRound.clear();
              previousRound.clear();
              running = false;
              log.info("Core seems to be shutting down, stopping importer");
            } else if (core.getCoreContainer().getZkController() != null &&
                core.getCoreContainer().getZkController().getZkClient().getConnectionManager().isLikelyExpired()) {
              currentRound.clear();
              previousRound.clear();
              running = false;
              log.info("ZK connection is likely expired, stopping importer");
            }
          } else {
            log.info("No records received");
          }
        } finally {
          // Deregister the update when done
          core.getSolrCoreState().deregisterInFlightUpdate();
        }
      }
    } catch (Throwable e) {
      log.error("Error encountered while running importer on core {}", core.getName(), e);
    }

    log.info("KafkaImporter finished");
  }

  /**
   * Commits most recent offsets to Kafka asynchronously.
   */
  private void commit(Consumer<String, SolrDocument> consumer, Map<TopicPartition, OffsetAndMetadata> commitData) {
    if (commitData.isEmpty()) {
      log.info("No commit data for this round, skipping sync commit");
      return;
    }

    log.info("Committing previous offsets back to Kafka after {} delay", commitInterval);
    try {
      consumer.commitSync(commitData);
    } catch (CommitFailedException e) {
      log.error("Commit failed", e);
    }
  }

  /**
   * Gets the consumer group lag. Static so that info can be retrieved without actually needing an instance of this
   * class to run.
   *
   * @param kafkaBroker The Kafka broker connect string
   * @param topics The list of topics that offsets should be returned for
   * @return a map of topic, partition, and group name to offset
   */
  public static Map<String, Long> getConsumerGroupLag(String kafkaBroker, List<String> topics) {
    final Properties props = new Properties();
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker);

    // Create an Admin to use
    try (final Admin admin = Admin.create(props)) {
      final Map<String, Long> map = new HashMap<>();
      final Map<TopicPartition, Long> ends = new HashMap<>();

      // Get all consumer groups
      Collection<ConsumerGroupListing> groups = admin.listConsumerGroups().valid().get(10000, TimeUnit.MILLISECONDS);

      // Loop through each of the consumer groups
      for (ConsumerGroupListing group : groups) {
        // Skip the group if it isn't an importer's group
        if (!group.groupId().contains(KAFKA_IMPORTER_GROUP)) {
          continue;
        }

        // Get the offsets for that group
        final Map<TopicPartition, OffsetAndMetadata> offsets = admin.listConsumerGroupOffsets(group.groupId())
            .partitionsToOffsetAndMetadata().get(10000, TimeUnit.MILLISECONDS);

        // If the ends map hasn't been set, add all known ends to it -- this is so that we only request this info once
        if (ends.isEmpty()) {
          ends.putAll(
              admin.listOffsets(
                  offsets.entrySet().stream().collect(
                      Collectors.toMap(Map.Entry::getKey, o -> OffsetSpec.latest()))).all()
                  .get(10000, TimeUnit.MILLISECONDS)
                  .entrySet().stream().filter(offset -> topics.contains(offset.getKey().topic()))
                  .collect(Collectors.toMap(Map.Entry::getKey, o -> o.getValue().offset())));
        }

        // Calculate the lag and add it to the map
        offsets.entrySet().stream().filter(val -> ends.containsKey(val.getKey()))
            .forEach(k -> map.put(k.getKey() + ":" + group.groupId(), ends.get(k.getKey()) - k.getValue().offset()));
      }
      return map;
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * Gets the end offsets for each consumer group. Static so that info can be retrieved without actually needing an
   * instance of this class to run.
   *
   * @param kafkaBroker The Kafka broker connect string
   * @param topic The topics that offsets should be returned for
   * @return a map of topic, partition, and group name to offset
   */
  public static Map<String, Long> getEndOffsets(String kafkaBroker, String topic) {
    final Properties props = new Properties();
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker);

    // Create an Admin to use
    try (final Admin admin = Admin.create(props)) {
      KafkaFuture<TopicDescription> future = admin.describeTopics(Collections.singletonList(topic))
          .values().get(topic);
      if (future == null) {
        throw new IllegalArgumentException("Unknown topic provided: " + topic);
      }

      Map<TopicPartition, OffsetSpec> partitions = future.get(5, TimeUnit.SECONDS).partitions()
          .stream().collect(Collectors.toMap(entry -> new TopicPartition(topic, entry.partition()),
              entry -> OffsetSpec.latest()));
      return admin.listOffsets(partitions).all().get(5, TimeUnit.SECONDS)
          .entrySet().stream()
          .collect(Collectors.toMap(
              o -> o.getKey().topic() + o.getKey().partition(),
              o -> o.getValue().offset()));
    } catch (InterruptedException | TimeoutException | ExecutionException e) {
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

    // Creates the importer group name depending on the all shard routing state and cloud mode cluster status
    final String group = String.format("%s:%s%s", KAFKA_IMPORTER_GROUP,
        (core.getCoreDescriptor().getCollectionName() == null ? core.getName() : core.getCoreDescriptor().getCollectionName()),
        (ignoreShardRouting ? ":" + core.getName() : ""));

    props.putIfAbsent(ConsumerConfig.GROUP_ID_CONFIG, group);
    props.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.putIfAbsent(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    props.putIfAbsent(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, kafkaPollInterval);
    props.putIfAbsent(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, kafkaMaxPollRecords);

    // Get the value deserializer based on the data type field from the SerdeFactory
    props.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SerdeFactory.getDeserializer(dataType).getName());

    // Set the AUTO_OFFSET_RESET_CONFIG to "earliest" or "latest" depending on autoOffsetResetBeginning field
    props.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetResetBeginning ? "earliest" : "latest");

    // Set the class loader to null because the KafkaConsumer isn't available on Solr class loaders
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(null);
    KafkaConsumer<String, SolrDocument> consumer = new KafkaConsumer<>(props);
    // Reset to what it was before when done
    Thread.currentThread().setContextClassLoader(loader);

    consumer.subscribe(topicNames);
    return consumer;
  }

  /**
   * Convert a {@code Map<String, Object>} to a {@link SolrInputDocument}
   * ({@link SolrDocument}s are {@code Map<String, Object>}).
   *
   * @param doc The doc to convert
   * @return the constructed {@link SolrInputDocument}
   */
  public static SolrInputDocument convertToInputDoc(Map<String, Object> doc) {
    SolrInputDocument inDoc = new SolrInputDocument();

    for (String name : doc.keySet()) {
      inDoc.addField(name, doc.get(name));
    }

    return inDoc;
  }

}
