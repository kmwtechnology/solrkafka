package org.apache.solr.update;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kmwllc.solr.solrkafka.datatype.json.JsonDeserializer;
import com.kmwllc.solr.solrkafka.datatype.json.JsonSerializer;
import com.kmwllc.solr.solrkafka.datatype.solr.SolrDocumentSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.solr.common.SolrDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * A Small utility class to generate some json solr documents into a kafka topic.
 */
public class JsonKafkaPublisher {

	private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
	
	private final static String TOPIC = "testtopic";
	private final static String BOOTSTRAP_SERVERS = "localhost:9092";
	private final static String CLIENT_ID = "KafkaExampleProducer";
	public int sendMessageCount = 1000000;

	public static void main(String[] args) throws Exception {
		JsonKafkaPublisher pub = new JsonKafkaPublisher();
//		pub.runProducer();
		pub.runShardProducer();
	}

	private void runShardProducer() throws Exception {
		final Producer<String, Object> producer = createProducer();
		long time = System.currentTimeMillis();
		final String[] ids = new String[] {"doc_4e1dd29c-85fc-4221-a25a-b84fcf24f4af",
				"doc_ff595762-b312-489a-8720-a8157855f781"};
		try {
			for (String id : ids) {
				Map<String, Object> doc = createSampleDocument(id);
				ProducerRecord<String, Object> record = new ProducerRecord<>(TOPIC, id, doc);
				// It's probably faster if we just send and don't bother with the get.  maybe just do it on the last one?
				// producer.send(record);
				producer.send(record).get();
				//log.info("sent record(key={} value={}) meta(partition={}, offset={}) time={}", record.key(), record.value(), metadata.partition(), metadata.offset(), elapsedTime);
			}
		} finally {
			producer.flush();
			producer.close();
		}
		long elapsedTime = System.currentTimeMillis() - time;
		System.out.println("Sent " + ids.length + " Rate " + 1000.0 * elapsedTime);
	}

	private static Producer<String, Object> createProducer() {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVERS);
		props.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
		return new KafkaProducer<>(props);
	}

	public void runProducer() throws Exception {
		final Producer<String, Object> producer = createProducer();
		long time = System.currentTimeMillis();
		try {
			long i = 0;
			for (long index = time; index < time + sendMessageCount; index++) {
				String docId = "doc_" + UUID.randomUUID().toString();
				Map<String, Object> doc = createSampleDocument(docId);
				ProducerRecord<String, Object> record = new ProducerRecord<>(TOPIC, docId, doc);
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
	
	private static Map<String, Object> createSampleDocument(String docId) throws IOException {
		Map<String, Object> doc = new HashMap<>();
		doc.put("id", docId);
		doc.put("title", "Hi mom");
		doc.put("date", new Date().toString());
		return doc;
	}

}