package com.kmwllc.solr.solrkafka;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.solr.common.SolrDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Small helper thread that will iterate SolrDocuments from a kafka topic and buffer them.
 * This thread returns an iterator that will block on next until some data has been received.
 * 
 * @author kwatters
 *
 * @param <T>
 */
public class KafkaIterator<T> implements Iterator<Map<String,Object>>, Runnable {

	private static final Logger log = LoggerFactory.getLogger(KafkaIterator.class);
	public final Thread consumerThread;
	private final Consumer<String, SolrDocument> consumer;
	private final LinkedBlockingQueue<SolrDocument> queue;
	private long pollTimeout = 1000;
	private volatile boolean running = false;
	public boolean readFullyAndExit = false;
	
	public KafkaIterator(Consumer<String, SolrDocument> consumer , LinkedBlockingQueue<SolrDocument> queue) { 
		this.consumer = consumer;
		this.queue = queue;
		consumerThread = new Thread(this, "KafkaConsumerThread");
		consumerThread.start();
		running = true;
		log.info("Kafka iterator created and started..");
	}

	@Override
	public boolean hasNext() {
		// Always true for kafka.. assume that a message can come in 
		// TODO: if the connection is severed? perhaps we might want to return false?
		// we could have a mode where this iterator will say it's done once it's caught up.
		if (!running && readFullyAndExit) {
			if (queue.size() > 0) {
				return true;
			} else {
				return false;
			}
		} else {
			return true;
		}
	}

	@Override
	public Map<String,Object> next() {
		// In the background there is a thread polling and putting messages on the blocking queue
		// This method blocks until something is available in the queue.
		SolrDocument o = null;
		while(o == null) {
			// grab the next element in the queue to return.
			try {
				o = queue.poll(pollTimeout, TimeUnit.MILLISECONDS);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				log.error("Kafka Iterator Interrupted. ", e);
				running = false;
				break;
			}
			// o = (SolrDocument)queue.poll();
		}
		// TODO: How are children documents going to be represented/handled?
		return o;
	}

	@Override
	public void run() {
		// now, we need to consume and put on queue.
		log.info("Staring Kafka consumer thread.");
		while (running) {
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
					// log.info("Consumer Record:({}, {}, {}, {}) Queue Size: ({})", record.key(), record.value(), record.partition(), record.offset(), queue.size());
					// System.out.printf("Consumer Record:(%d, %s, %d, %d) Queue Size: (%d)\n");
					try {
						queue.put(record.value());
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
		// TODO: when do we commit sync  and what do we do about the current queue size?  we should drain that
		// lastly we still need to figure out how and when the consumer resets.
		log.info("Kafka Consumer Thread Exiting.");
	}

	public void stop() {
		// interrupt this consumer thread.
		running = false;
		try {
			// TODO: something better than just waiting for the previous poll attempt to finish.
			Thread.sleep(pollTimeout + 100);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//consumerThread.interrupt();
		// TODO: should I join the thread here or something?
		// We should probably wait a second?  to let the previous poll call finish?
	}

}
