package com.kmwllc.solr.solrkafka.requesthandler.consumerhandlers;

import org.apache.kafka.clients.consumer.CommitFailedException;
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
	 * @param readFullyAndExit If true, exits when no documents are received from Kafka after the {@link this#pollTimeout}
	 *                         expires
	 */
	AsyncKafkaConsumerHandler(Properties consumerProps, String topic, boolean fromBeginning, boolean readFullyAndExit) {
		// TODO: do we want to keep this capacity for the queue?
		super(consumerProps, topic, fromBeginning, readFullyAndExit, 2000);
		consumerThread = new Thread(this, "KafkaConsumerThread");
		consumerThread.start();
		running = true;
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
		while (consumerThread != null && consumerThread.isAlive() && running && inputQueue.size() == 0) {
			Thread.onSpinWait();
		}
		return inputQueue.size() > 0;
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
		// TODO: maybe get rid of locking and let run() loop handle? losing offsets might not be important here
    // Attempt to add commits to the pendingCommits map if the thread is still running and not preparing
		// to exit
		if (consumerThread.isAlive() && consumerSemaphore.tryAcquire()) {
			pendingCommits.putAll(commit);
		  consumerSemaphore.release();
		} else {
			// Consumer thread is preparing to exit (or already exited), wait until it is confirmed
			while (consumerThread.isAlive()) {
				Thread.onSpinWait();
			}
			commitToConsumer(commit);
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
			  commitToConsumer(pendingCommits);
				pendingCommits.clear();
			}
			if (!running) {
				break;
			}
			loadSolrDocs();
		}

		// Begin to shut down. Locks are not released to prevent commits back to Kafka from being lost
    acquireSemaphore();
		commitToConsumer(pendingCommits);
		pendingCommits.clear();
		// lastly we still need to figure out how and when the consumer resets.
		log.info("Kafka Consumer Thread Exiting.");
	}

	/**
	 * Forcefully stop the {@link KafkaConsumerHandler} if not already done. Waits {@link this#pollTimeout} + 1 second before
	 * interrupting the thread if it doesn't shut down before then. Closes the {@link Consumer} when finished.
	 */
	@Override
	public void stop() {
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
}
