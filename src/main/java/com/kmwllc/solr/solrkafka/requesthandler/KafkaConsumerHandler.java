package com.kmwllc.solr.solrkafka.requesthandler;

import com.kmwllc.solr.solrkafka.serde.SolrDocumentDeserializer;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.solr.common.SolrDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Small helper thread that will iterate SolrInputDocuments from a kafka topic and buffer them.
 * This thread returns an iterator that will block on hasNext until some data has been received.
 */
public class KafkaConsumerHandler implements Iterator<DocumentData>, Runnable {

	private static final Logger log = LoggerFactory.getLogger(KafkaConsumerHandler.class);
	public final Thread consumerThread;
	private static Consumer<String, SolrDocument> consumer;
	private final LinkedBlockingQueue<DocumentData> inputQueue;
	private final long pollTimeout = 1000;
	private volatile boolean running;
	public boolean readFullyAndExit;
	private final Semaphore exitSemaphore = new Semaphore(1);
	private final Map<TopicPartition, OffsetAndMetadata> pendingCommits = new ConcurrentHashMap<>();
	private boolean exited = false;

	/**
	 * @param consumerProps The {@link Properties} that should be used to create a {@link KafkaConsumer}.
	 * @param topic The topic to pull from
	 * @param fromBeginning If true, pull new entries from the beginning of the topic's history
	 * @param readFullyAndExit If true, exits when no documents are received from Kafka after the {@link this#pollTimeout}
	 *                         expires
	 */
	public KafkaConsumerHandler(Properties consumerProps, String topic, boolean fromBeginning, boolean readFullyAndExit) {
		this.readFullyAndExit = readFullyAndExit;
		consumer = createConsumer(consumerProps);
		consumer.subscribe(Collections.singletonList(topic));
		if (fromBeginning) {
			consumer.poll(0);
			consumer.seekToBeginning(consumer.assignment());
		}
		// TODO: do we want to keep this capacity?
		inputQueue = new LinkedBlockingQueue<>(100);
		consumerThread = new Thread(this, "KafkaConsumerThread");
		consumerThread.start();
		running = true;
		log.info("Kafka iterator created and started..");
	}

	/**
	 * Creates a new {@link KafkaConsumer} using the given properties. If a previous consumer was set,
	 * it is closed before creating a new one.
	 *
	 * @param props The properties to be used when creating the consumer
	 * @return an initialized consumer
	 */
	private Consumer<String, SolrDocument> createConsumer(Properties props) {
		if (consumer != null) {
			consumer.close();
		}

		String bootStrapServers = "localhost:9092";
		props.putIfAbsent(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
		String consumerGroupId = "SolrKafkaConsumer";
		props.putIfAbsent(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
		props.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SolrDocumentDeserializer.class.getName());
		// How do we force the offset ?
		props.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		// https://www.programmersought.com/article/37481195666/
		// https://stackoverflow.com/questions/1771679/difference-between-threads-context-class-loader-and-normal-classloader/36228195#36228195
		// Override class loader for creating KafkaConsumer
		ClassLoader loader = Thread.currentThread().getContextClassLoader();
		Thread.currentThread().setContextClassLoader(null);
		// Create the consumer using props.
		KafkaConsumer<String, SolrDocument> consumer = new KafkaConsumer<>(props);
		Thread.currentThread().setContextClassLoader(loader);
		return consumer;
	}

	/**
	 * Blocks until it is determined that new documents can or cannot be returned.
	 *
	 * @return true if documents will be returned by {@link this#next()}, false otherwise
	 */
	@Override
	public boolean hasNext() {
	  // Wait until the thread exits, the thread is preparing to exit, or there are documents in the queue
		// to be processed
		while (consumerThread != null && consumerThread.isAlive() && running && inputQueue.size() == 0) {
			Thread.onSpinWait();
		}
		return inputQueue.size() > 0;
	}

	@Override
	public DocumentData next() {
		// In the background there is a thread polling and putting messages on the blocking queue
		// This method blocks until something is available in the queue.
		// TODO: not sure if we need to wait for documents to be returned anymore here, since they are
		//  guaranteed with hasNext()
		DocumentData o = null;
		while(o == null) {
			// grab the next element in the queue to return.
			try {
				o = inputQueue.poll(pollTimeout, TimeUnit.MILLISECONDS);
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
	 * Commits the offsets provided back to Kafka, preferring to use the {@link this#consumerThread} before
	 * committing directly in this method. If this method attempts to commit while {@link this#run()} is
	 * also working with the {@link Consumer}, a {@link java.util.ConcurrentModificationException}
	 * will be thrown.
	 *
	 * @param commit The offsets and corresponding partitions to commit
	 */
	public void commitOffsets(Map<TopicPartition, OffsetAndMetadata> commit) {
		// TODO: maybe get rid of this and let run() loop handle? losing offsets might not be important here
    // Attempt to add commits to the pendingCommits map if the thread is still running and not preparing
		// to exit
		if (consumerThread.isAlive() && exitSemaphore.tryAcquire()) {
			pendingCommits.putAll(commit);
		  exitSemaphore.release();
		} else {
			// Consumer thread is preparing to exit (or already exited), wait until it is confirmed
			while (consumerThread.isAlive()) {
				Thread.onSpinWait();
			}
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

	/**
	 * Start loading documents into the {@link this#inputQueue} asynchronously.
	 */
	@Override
	public void run() {
		// now, we need to consume and put on queue.
		log.info("Staring Kafka consumer thread.");
		// Commit any pending commits
		while (running) {
			if (!pendingCommits.isEmpty()) {
				consumer.commitSync(pendingCommits);
				pendingCommits.clear();
			}
			if (!running) {
				break;
			}
			final ConsumerRecords<String, SolrDocument> consumerRecords = consumer.poll(pollTimeout);
			// were we interrupted since the pollTimeout.. if so.. quick exit here.
			if (!running) {
				log.info("Thread isn't running.. breaking out!");
				break;
			}
			if (consumerRecords.count() > 0) {
				log.info("Processing consumer records. {}", consumerRecords.count());
				for (ConsumerRecord<String, SolrDocument> record : consumerRecords) {
					try {
						TopicPartition partInfo = new TopicPartition(record.topic(), record.partition());
						OffsetAndMetadata offset = new OffsetAndMetadata(record.offset());
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

		// Begin to shut down. Locks are used to prevent commits back to Kafka from being lost
		try {
			exitSemaphore.acquire();
		} catch (InterruptedException e) {
			consumerThread.interrupt();
		}
		try {
			consumer.commitSync(pendingCommits);
		} catch (CommitFailedException e) {
			log.error("Error occurred with index commit", e);
			running = false;
			return;
		}
		pendingCommits.clear();
		// lastly we still need to figure out how and when the consumer resets.
		log.info("Kafka Consumer Thread Exiting.");
		exited = true;
	}

	/**
	 * Forcefully stop the Kafka consumer. Waits {@link this#pollTimeout} + 1 second before interrupting the thread if
	 * it doesn't shut down before then.
	 */
	public void stop() {
		exited = true;
		// interrupt this consumer thread.
		running = false;
		try {
			// TODO: something better than just waiting for the previous poll attempt to finish.
			Thread.sleep(pollTimeout + 1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		if (consumerThread.isAlive()) {
			consumerThread.interrupt();
		}
		// TODO: should I join the thread here or something?
		// We should probably wait a second?  to let the previous poll call finish?
		consumer.close();
	}

	public boolean exited() {
		return exited;
	}
}
