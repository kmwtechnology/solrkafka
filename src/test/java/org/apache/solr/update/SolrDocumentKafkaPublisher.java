package org.apache.solr.update;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.solr.common.SolrDocument;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kmwllc.solr.solrkafka.SolrDocumentSerializer;

import java.lang.invoke.MethodHandles;
import java.util.Date;
import java.util.Properties;
import java.util.UUID;

/**
 * A Small utility class to generate some solr documents into a kafka topic.
 * 
 * @author kwatters
 *
 */
public class SolrDocumentKafkaPublisher {

	private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
	
	private final static String TOPIC = "testtopic";
	private final static String BOOTSTRAP_SERVERS = "localhost:9092";
	private final static String CLIENT_ID = "KafkaExampleProducer";
	public int sendMessageCount = 1000000;

	private static Producer<String, SolrDocument> createProducer() {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVERS);
		props.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, SolrDocumentSerializer.class.getName());
		return new KafkaProducer<>(props);
	}

	@Test
	public void runProducer() throws Exception {
		final Producer<String, SolrDocument> producer = createProducer();
		long time = System.currentTimeMillis();
		try {
			long i = 0;
			for (long index = time; index < time + sendMessageCount; index++) {
				String docId = "doc_" + UUID.randomUUID().toString();
				SolrDocument doc = createSampleDocument(docId);
				ProducerRecord<String, SolrDocument> record = new ProducerRecord<String, SolrDocument>(TOPIC, docId, doc);
				// It's probably faster if we just send and don't bother with the get.  maybe just do it on the last one?
				// producer.send(record);
				RecordMetadata metadata = producer.send(record).get();
				long elapsedTime = System.currentTimeMillis() - time;
				//log.info("sent record(key={} value={}) meta(partition={}, offset={}) time={}", record.key(), record.value(), metadata.partition(), metadata.offset(), elapsedTime);
				i++;
				if (i % 10000 == 0) {
					System.out.println("Sent " + i + " Rate " + 1000.0 * i / elapsedTime);
				}
			}
		} finally {
			producer.flush();
			producer.close();
		}
	}
	
	private static SolrDocument createSampleDocument(String docId) {
		SolrDocument doc = new SolrDocument();
		doc.setField("id", docId);
		doc.setField("title", "Hi mom");
		doc.setField("date", new Date());
		return doc;
	}

}