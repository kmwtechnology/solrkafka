package com.kmwllc.solr.solrkafka.requesthandler.consumerhandlers;

import com.kmwllc.solr.solrkafka.datatypes.SerdeFactory;
import com.kmwllc.solr.solrkafka.queue.MyQueue;
import com.kmwllc.solr.solrkafka.datatypes.DocumentData;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Semaphore;

public abstract class KafkaConsumerHandler implements Iterator<DocumentData> {
  private static final Logger log = LogManager.getLogger(KafkaConsumerHandler.class);
  protected Consumer<String, Map<String, Object>> consumer;
  protected static final long POLL_TIMEOUT = 1000;
  protected boolean readFullyAndExit;
  private boolean alreadyRun = false;
  protected volatile boolean running = false;
  protected final MyQueue<DocumentData> inputQueue;
  protected final Semaphore consumerSemaphore = new Semaphore(1);

  /**
   * @param consumerProps The properties used to initialize the {@link Consumer}
   * @param topic The topic to listen on
   * @param fromBeginning true the topic should be read from the beginning
   * @param readFullyAndExit true if the consumer should exit after reaching the end of the topic's history
   * @param queue The {@link MyQueue} to hold pulled documents in
   */
  protected KafkaConsumerHandler(Properties consumerProps, String topic, boolean fromBeginning, boolean readFullyAndExit,
                                 MyQueue<DocumentData> queue, String dataType) {
    this.readFullyAndExit = readFullyAndExit;
    this.inputQueue = queue;
    consumer = createConsumer(consumerProps, dataType);
    consumer.subscribe(Collections.singletonList(topic));
    if (fromBeginning) {
      consumer.poll(0);
      consumer.seekToBeginning(consumer.assignment());
    }
  }

  /**
   * Get a new instance of a {@link KafkaConsumerHandler}. A {@link SyncKafkaConsumerHandler} is returned as default.
   *
   * @param type The desired type, or null/empty string for default
   * @param consumerProps The properties used to initialize the {@link Consumer}
   * @param topic The topic to listen on
   * @param fromBeginning true the topic should be read from the beginning
   * @param readFullyAndExit true if the consumer should exit after reaching the end of the topic's history
   * @return a new {@link KafkaConsumerHandler} instance
   */
  public static KafkaConsumerHandler getInstance(String type, Properties consumerProps, String topic,
                                                 boolean fromBeginning, boolean readFullyAndExit, String dataType) {
    if (type == null || type.isBlank()) {
      log.info("No type provided, defaulting to sync");
      return new SyncKafkaConsumerHandler(consumerProps, topic, fromBeginning, readFullyAndExit, dataType);
    } else if (type.equalsIgnoreCase("async") || type.equalsIgnoreCase("asynchronous")) {
      return new AsyncKafkaConsumerHandler(consumerProps, topic, fromBeginning, readFullyAndExit, dataType);
    }
    if (!type.equalsIgnoreCase("sync") && !type.equalsIgnoreCase("synchronous")) {
      log.warn("Unknown type provided [{}], defaulting to sync", type);
    }
    return new SyncKafkaConsumerHandler(consumerProps, topic, fromBeginning, readFullyAndExit, dataType);
  }

  /**
   * Stop the {@link KafkaConsumerHandler}. Closes the {@link Consumer} when done.
   */
  public abstract void stop();

  /**
   * A method meant to be called to determine if this {@link KafkaConsumerHandler} has already run. When called
   * will return the previous value and set the {@link this#alreadyRun} field to true.
   *
   * @return false if this method has not been called before, true otherwise
   */
  public boolean hasAlreadyRun() {
    boolean hasRun = alreadyRun;
    alreadyRun = true;
    return hasRun;
  }

  /**
   * Commits the offsets provided back to Kafka,
   *
   * @param commit The offsets and corresponding partitions to commit
   */
  public abstract void commitOffsets(Map<TopicPartition, OffsetAndMetadata> commit);

  public boolean isRunning() {
    return this.running;
  }

  @Override
  public DocumentData next() {
    // In the background there is a thread polling and putting messages on the blocking queue
    // This method blocks until something is available in the queue.
    // TODO: not sure if we need to wait for documents to be returned anymore here, since they are
    //  guaranteed with hasNext()
    // TODO: we could have multiple threads inserting into solr if that's a bottleneck
    DocumentData o = null;
    while(o == null) {
      // grab the next element in the queue to return.
      try {
        o = inputQueue.poll();
      } catch (InterruptedException e) {
        log.error("Kafka Iterator Interrupted. ", e);
        running = false;
        break;
      }
    }
    // TODO: How are children documents going to be represented/handled?
    return o;
  }

  /**
   * Gets the next batch of Solr documents from Kafka, returning false immediately if {@link this#running} is false.
   * If {@link this#readFullyAndExit} is true, then {@code running} is set to false.
   *
   * @return true if documents were added to {@link this#inputQueue}, false otherwise
   */
  protected void loadSolrDocs() {
    // Locking here to prevent commits back to Kafka if it happens at the same time as this
    acquireSemaphore();
    final ConsumerRecords<String, Map<String, Object>> consumerRecords = consumer.poll(POLL_TIMEOUT);
    consumerSemaphore.release();
    // were we interrupted since the pollTimeout.. if so.. quick exit here.
    if (!running) {
      log.info("Thread isn't running.. breaking out!");
      return;
    }
    if (consumerRecords.count() > 0) {
      log.info("Processing consumer records. {}", consumerRecords.count());
      for (ConsumerRecord<String, Map<String, Object>> record : consumerRecords) {
        TopicPartition partInfo = new TopicPartition(record.topic(), record.partition());
        OffsetAndMetadata offset = new OffsetAndMetadata(record.offset() + 1);
        try {
          // TODO: may not actually need blocking here
          inputQueue.put(new DocumentData(record.value(), partInfo, offset));
        } catch (InterruptedException e) {
          running = false;
          log.info("Kafka consumer thread interrupted.", e);
          break;
        }
      }
    } else {
      // no records read.. if we are reading from the beginning, we can assume we have caught up.
      // TODO: that's probably simplistic, we need to know if we are caught up for all partitions that we subscribe to!!!
      if (readFullyAndExit) {
        running = false;
      }
    }
  }

  /**
   * Creates a new {@link KafkaConsumer} using the given properties. If a previous consumer was set,
   * it is closed before creating a new one.
   *
   * @param props The properties to be used when creating the consumer
   * @return an initialized consumer
   */
  private Consumer<String, Map<String, Object>> createConsumer(Properties props, String dataType) {
    if (consumer != null) {
      consumer.close();
    }

    String bootStrapServers = "localhost:9092";
    props.putIfAbsent(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
    String consumerGroupId = "SolrKafkaConsumer";
    props.putIfAbsent(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
    props.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SerdeFactory.getDeserializer(dataType).getName());
    // How do we force the offset ?
    props.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    // This value must be larger than the autocommit interval used for Solr
    props.putIfAbsent(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 16000);
    // https://www.programmersought.com/article/37481195666/
    // https://stackoverflow.com/questions/1771679/difference-between-threads-context-class-loader-and-normal-classloader/36228195#36228195
    // Override class loader for creating KafkaConsumer
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(null);
    // Create the consumer using props.
    KafkaConsumer<String, Map<String, Object>> consumer = new KafkaConsumer<>(props);
    Thread.currentThread().setContextClassLoader(loader);
    return consumer;
  }

  protected void acquireSemaphore() {
    try {
      consumerSemaphore.acquire();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  protected void commitToConsumer(Map<TopicPartition, OffsetAndMetadata> commit) {
    try {
      consumer.commitSync(commit);
    } catch (CommitFailedException e) {
      // TODO: what should be done when a CommitFailedException occurs?
      // Seems to be happening because the max Kafka poll interval was exceeded
      log.error("Error occurred with index commit", e);
      running = false;
    }
  }
}
