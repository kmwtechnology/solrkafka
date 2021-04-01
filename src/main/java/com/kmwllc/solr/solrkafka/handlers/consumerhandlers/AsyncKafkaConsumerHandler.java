package com.kmwllc.solr.solrkafka.handlers.consumerhandlers;

import com.kmwllc.solr.solrkafka.importers.Status;
import com.kmwllc.solr.solrkafka.queue.BlockingMyQueue;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Small helper thread that will iterate SolrInputDocuments from a kafka topic and buffer them.
 * This thread returns an iterator that will block on hasNext until some data has been received.
 */
public class AsyncKafkaConsumerHandler extends KafkaConsumerHandler implements Runnable {

	private static final Logger log = LoggerFactory.getLogger(AsyncKafkaConsumerHandler.class);
	public final Thread consumerThread;
	private final Map<TopicPartition, OffsetAndMetadata> pendingCommits = new ConcurrentHashMap<>();

	/**
	 * @param consumerProps The {@link Properties} that should be used to create a {@link KafkaConsumer}.
	 * @param topic The topic to pull from
	 * @param fromBeginning If true, pull new entries from the beginning of the topic's history
	 * @param readFullyAndExit If true, exits when no documents are received from Kafka after the {@link KafkaConsumerHandler#POLL_TIMEOUT}
	 *                         expires
	 */
	AsyncKafkaConsumerHandler(Properties consumerProps, String topic, boolean fromBeginning, boolean readFullyAndExit,
														String dataType) {
		// TODO: do we want to keep this capacity for the queue?
		super(consumerProps, topic, fromBeginning, readFullyAndExit, new BlockingMyQueue<>(2000, POLL_TIMEOUT),
				dataType);
		consumerThread = new Thread(this, "KafkaConsumerThread");
		consumerThread.start();
		status = Status.RUNNING;
		log.info("Kafka iterator created and started..");
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
		while (consumerThread != null && consumerThread.isAlive() && status.isOperational() && inputQueue.isEmpty()) {
			log.debug("Input queue is empty, waiting for documents to populate");
			// TODO: do we want to yield here or spin?
//			Thread.onSpinWait();
      Thread.yield();
		}
		return !inputQueue.isEmpty();
	}

	/**
	 * Commits the offsets provided back to Kafka, preferring to use the {@link this#consumerThread} before
	 * committing directly in this method. If this method attempts to commit while {@link this#run()} is
	 * also working with the {@link Consumer}, a {@link java.util.ConcurrentModificationException}
	 * will be thrown.
   *
	 * @param commit The offsets and corresponding partitions to commit
	 */
	@Override
	public void commitOffsets(Map<TopicPartition, OffsetAndMetadata> commit) {
    // Attempt to add commits to the pendingCommits map if the thread is still running
    pendingCommits.putAll(commit);
    if (status.isOperational()) {
      log.info("Committing back to Kafka asynchronously in main thread");
    	return;
		}

    log.info("Waiting to shut down before committing to Kafka synchronously");
    while (consumerThread.isAlive()) {
    	Thread.onSpinWait();
		}
    commitToConsumer(pendingCommits);
	}

	/**
	 * Start loading documents into the {@link this#inputQueue} asynchronously.
	 */
	@Override
	public void run() {
		// now, we need to consume and put on queue.
		log.info("Staring Kafka consumer thread.");
		// Commit any pending commits
		while (status.isOperational()) {
			// Not afraid to lose the occasional commit because it will be committed in the future in normal operation
			// TODO: make sure this gets cleared during rewind
		  if (!pendingCommits.isEmpty()) {
		  	log.info("Committing back to Kafka synchronously in main thread");
		  	commitToConsumer(pendingCommits);
		  	pendingCommits.clear();
			}
			if (!status.isOperational()) {
				break;
			}
			loadSolrDocs();
		}

		// lastly we still need to figure out how and when the consumer resets.
		log.info("Kafka Consumer Thread Exiting.");
	}

	/**
	 * Forcefully stop the {@link KafkaConsumerHandler} if not already done. Waits {@link KafkaConsumerHandler#POLL_TIMEOUT} + 1 second before
	 * interrupting the thread if it doesn't shut down before then. Closes the {@link Consumer} when finished.
	 */
	@Override
	public void stop() {
		log.info("Stopping consumer handler and closing consumer");
		// interrupt this consumer thread.
    status = Status.DONE;
		try {
			// TODO: something better than just waiting for the previous poll attempt to finish.
			Thread.sleep(POLL_TIMEOUT + 1000);
		} catch (InterruptedException e) {
			log.error("Sleep for shutdown interrupted", e);
		}
		if (consumerThread.isAlive()) {
		  log.info("Consumer thread not done after sleep timeout; interrupting thread");
			consumerThread.interrupt();
		}
		// TODO: should I join the thread here or something?
		// We should probably wait a second?  to let the previous poll call finish?
		if (!isClosed) {
			consumer.close();
			isClosed = true;
		}
	}
}
