package com.kmwllc.solr.solrkafka.importer;

import com.kmwllc.solr.solrkafka.datatype.DocumentData;
import com.kmwllc.solr.solrkafka.datatype.SerdeFactory;
import com.kmwllc.solr.solrkafka.datatype.solr.SolrDocumentDeserializer;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Slice;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Imports documents from a Kafka {@link Consumer} in a separate thread and adds them to Solr using the same
 * functionality as the (soon-to-be-deprecated) Document Import Handler.
 */
public class KafkaImporter implements Runnable, Importer {
  private static final Logger log = LogManager.getLogger(KafkaImporter.class);
  private volatile SolrCore core;
  private volatile UpdateRequestProcessor updateHandler;
  private final Consumer<String, SolrDocument> consumer;
  private final Duration pollTimeout = Duration.ofMillis(1000);
  private static final NamedList<String> SOLR_REQUEST_ARGS = new NamedList<>();
  private final Thread thread;
  private volatile Status status = Status.NOT_STARTED;
  private final boolean readFullyAndExit;
  private volatile boolean isClosed = false;
  private final TemporalAmount commitInterval;
  private volatile boolean rewind;
  private final Map<String, Long> consumerGroupLag = new HashMap<>();
  private final boolean ignoreShardRouting;
  private final String topicName;
  private final String dataType;

  static {
    SOLR_REQUEST_ARGS.add("commitWithin", "1000");
    SOLR_REQUEST_ARGS.add("overwrite", "true");
    SOLR_REQUEST_ARGS.add("wt", "json");
  }

  /**
   * @param core The {@link SolrCore} to add documents to
   * @param readFullyAndExit {@code true} if this should exit after receiving no documents from Kafka (will not exit if paused)
   * @param fromBeginning {@code true} if this should start reading from the {@link Consumer}'s beginning
   * @param commitInterval The minimum number of MS between each Kafka offset commit
   * @param ignoreShardRouting {@code true} if all documents should be added to every shard
   */
  public KafkaImporter(SolrCore core, String topicName, boolean readFullyAndExit, boolean fromBeginning, long commitInterval,
                       boolean ignoreShardRouting, String dataType) {
    this.topicName = topicName;
    this.core = core;
    this.ignoreShardRouting = ignoreShardRouting;
    this.updateHandler = createUpdateHandler();
    this.readFullyAndExit = readFullyAndExit;
    this.commitInterval = Duration.ofMillis(commitInterval);
    rewind = fromBeginning;
    this.dataType = dataType;
    this.consumer = createConsumer();
    thread = new Thread(this, "KafkaImporter Async Runnable");
  }

  /**
   * Creates a {@link UpdateRequestProcessor} based off of the one provided by the {@link SolrCore}. If
   * {@link this#ignoreShardRouting} is {@code true}, then edit the chain provided by the core and insert our own
   * {@link DistributedShardUpdateProcessor} in place of the {@link DistributedShardUpdateProcessor}.
   *
   * @return A {@link UpdateRequestProcessor} instance
   */
  private UpdateRequestProcessor createUpdateHandler() {
    SolrParams params = new MultiMapSolrParams(Map.of());
    UpdateRequestProcessorChain chain = core.getUpdateProcessorChain(params);
    if (!ignoreShardRouting) {
      return chain.createProcessor(new LocalSolrQueryRequest(core, params), null);
    }

    List<UpdateRequestProcessorFactory> factories = new ArrayList<>(chain.getProcessors());
    for (int i = 0; i < factories.size(); i++) {
      UpdateRequestProcessorFactory factory = factories.get(i);
      if (factory instanceof DistributedUpdateProcessorFactory) {
        factory = new DistributedShardUpdateProcessor.DistributedShardUpdateProcessorFactory(true);
        factories.set(i, factory);
        break;
      }
    }

    return new UpdateRequestProcessorChain(factories, core)
        .createProcessor(new LocalSolrQueryRequest(core, params), null);
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

    // Let the importer try to stop on its own
    try {
      Thread.sleep(pollTimeout.toMillis() * 5);
    } catch (InterruptedException e) {
      log.error("Thread interrupted while stopping", e);
      status = Status.ERROR;
    }

    // Interrupt the thread if it couldn't shut down on its own
    if (thread != null && thread.isAlive()) {
      log.warn("Thread took too long to shut down; interrupting thread");
      thread.interrupt();
    }

    // Close the Kafka consumer if it hasn't been closed yet
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
    this.updateHandler = createUpdateHandler();
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

      // Handle pausing and resuming consumption from Kafka
      if (localStatus == Status.PAUSED && consumer.paused().size() != consumer.assignment().size()) {
        log.info("Pausing unpaused Kafka consumer assignments");
        consumer.pause(consumer.assignment());
      } else if (localStatus == Status.RUNNING && !consumer.paused().isEmpty()) {
        log.info("Resuming paused Kafka consumer assignments");
        consumer.resume(consumer.assignment());
      }

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
          add.solrDoc = DocumentData.convertToInputDoc(record.value());

          // Attempt to add the update
          try {
            updateHandler.processAdd(add);
          } catch (IOException e) {
            log.error("Couldn't add solr doc...", e);
            status = Status.ERROR;
          }
        }

      } else {
        log.info("No records received");

        // Exit if no documents are received, we're planning to readFullyAndExit, and we're not paused
        if (readFullyAndExit && localStatus != Status.PAUSED) {
          status = Status.DONE;
        }
      }

      // If commitInterval has elapsed, commit back to Kafka
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

  /**
   * Commits most recent offsets to Kafka asynchronously and updates consumer lag.
   */
  public void commit() {
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

  @Override
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
    props.putIfAbsent(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.putIfAbsent(ConsumerConfig.GROUP_ID_CONFIG, core.getName());
    props.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SerdeFactory.getDeserializer(dataType).getName());
    props.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.putIfAbsent(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 16000);

    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(null);
    KafkaConsumer<String, SolrDocument> consumer = new KafkaConsumer<>(props);
    Thread.currentThread().setContextClassLoader(loader);

    // TODO: should this be able to consume from multiple topics?
    consumer.subscribe(Collections.singletonList(topicName));
    return consumer;
  }

  /**
   * Determine if the provided command should be executed on this shard. If {@link this#ignoreShardRouting} is true,
   * then the command is immediately executed without performing shard routing checks.
   *
   * @param cmd The command to execute
   * @return true if the command should be executed on this shard
   */
  private boolean addToThisShard(AddUpdateCommand cmd) {
    if (ignoreShardRouting) {
      return true;
    }

    ZkController zkController = core.getCoreContainer().getZkController();
    ClusterState clusterState = zkController.getClusterState();
    CloudDescriptor cloudDesc = core.getCoreDescriptor().getCloudDescriptor();
    DocCollection coll = clusterState.getCollection(cloudDesc.getCollectionName());

    Slice slice = coll.getRouter().getTargetSlice(cmd.getIndexedIdStr(), cmd.getSolrInputDocument(),
        null, new MultiMapSolrParams(new HashMap<>()), coll);

    if (slice == null) {
      // No slice found.  Most strict routers will have already thrown an exception, so a null return is
      // a signal to use the slice of this core.
      // TODO: what if this core is not in the targeted collection?
      String shardId = cloudDesc.getShardId();
      slice = coll.getSlice(shardId);
      if (slice == null) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "No shard " + shardId + " in " + coll);
      }
    }

    return slice.getName().equals(cloudDesc.getShardId());
  }
}
