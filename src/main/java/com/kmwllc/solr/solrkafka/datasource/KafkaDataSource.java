package com.kmwllc.solr.solrkafka.datasource;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

import com.kmwllc.solr.solrkafka.serde.solr.SolrDocumentDeserializer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.handler.dataimport.Context;
import org.apache.solr.handler.dataimport.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka Source for the Data Import Handler
 * This source when started will attach to a kafka topic that contains SolrDocuments serialized with the JavaBinCodec.
 * Each of those kafka messages are deserialized and returned as a map of String to Object.
 * Specify the list of bootStrapServers and the kafka topic name to subscribe to.
 * The key for the kafka topic should be equal to the id of the solr document so that updates
 * for the same document end up in the same partition and therefore have ordered delivery.
 * 
 * This consumer uses the standard StringSerializer/Deserializer from Kafka for the key serde
 * It uses the SolrDocumentSerializer/Deserializer for the value serde
 * 
 * @author kwatters
 *
 */
public class KafkaDataSource extends DataSource<Iterator<Map<String, Object>>> {
	
	private static final Logger log = LoggerFactory.getLogger(KafkaDataSource.class);
	private final String bootStrapServers = "localhost:9092";
	private final String consumerGroupId = "SolrKafkaConsumer";
	private Consumer<String, SolrDocument> consumer = null;
	private KafkaIterator iter = null;
	private LinkedBlockingQueue<SolrDocument> queue = null;
	private final int queueSize = 100;
	public boolean fromBeginning = true;
	public boolean readFullyAndExit = false;

	@Override
	public void init(Context context, Properties initProps) {
		// Okie dokie.. here , i guess we've got to do what?  setup the properties .. connect to kafka/ subscribe to a topic.
		queue = new LinkedBlockingQueue<>(queueSize);
		if (initProps == null) 
			initProps = new Properties();
		consumer = createConsumer(initProps);
		log.info("Kafka Consumer created.");
		
		// If this is a full ingest, we should run from the beginning// o/w we should pick up from our last known offsets.
		// reset behavior for unknown offsets is/should be earliest.
		if (context != null && Context.FULL_DUMP.contentEquals(context.currentProcess())) {
			fromBeginning = true;
		} else if (context != null && Context.DELTA_DUMP.contentEquals(context.currentProcess())) {
			fromBeginning = false;
		} else {
			log.warn("Unknown mode, only FULL_DUMP and DELTA_DUMP supported, defaulting to full.");
			fromBeginning = true;
		}
		
	}

	private Consumer<String, SolrDocument> createConsumer(Properties props) {
		props.putIfAbsent(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
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

	@Override
	public Iterator<Map<String, Object>> getData(String topic) {
		// Create the iterator and return it?
		// TODO : maybe this should be created up front and just returned.
		// If this method is called often, we want to return the same iterator (likely?)
		// TODO:  I think we should create a new consumer here, and switch the "query" passed in to be the topic name?
		log.info("GetData called with topic: {}", topic);
		// Subscribe to the topic.
		consumer.subscribe(Collections.singletonList(topic));
		// If we are supposed to start from the beginning.. let's see if we can seek there.
		if (fromBeginning) {
			consumer.poll(0);
			consumer.seekToBeginning(consumer.assignment());
		}
		iter = new KafkaIterator(consumer, queue);
		// TODO: better config for this kafka iterator object.
		iter.readFullyAndExit = this.readFullyAndExit;
		return iter;
	}

	@Override
	public void close() {
		// TODO: close the iterator... connection to kafka.
		// TODO: check the size of the current queue.. flush it perhaps?  commit back to the kafka topic when it's empty?
		// consumer.close();
		// TODO: other cleanup? like the kafka iterator.
		// TODO: any cleanup with the iterator?  yes.. interrupt it and shut it down.
		iter.stop();
		consumer.close();
		// TODO what else can/should we close/commit?  
		// TOOD: maybe we only commit up to the last offset of the last consumed message.. 

	}

}
