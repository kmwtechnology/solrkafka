package org.apache.solr.update;

import com.kmwllc.solr.solrkafka.test.TestDocumentCreator;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.solr.common.SolrDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kmwllc.solr.solrkafka.datatype.solr.SolrDocumentSerializer;

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
//	private final static String TOPIC = "testtopic1";
	private final static String BOOTSTRAP_SERVERS = "localhost:9092";
	private final static String CLIENT_ID = "KafkaExampleProducer";
//	private static final int sendMessageCount = 25_000; // 25_000 for 10kb docs, 100_000 for .1kb docs
	private static final int sendMessageCount = 100_000; // 25_000 for 10kb docs, 100_000 for .1kb docs
//	private static final int maxDocumentSize = 10_000; // 10_000 is around 10kb msg size, 100 is around .1kb
	private static final int maxDocumentSize = 100; // 10_000 is around 10kb msg size, 100 is around .1kb
	private final TestDocumentCreator docs = new TestDocumentCreator(sendMessageCount, maxDocumentSize);

	public static void main(String[] args) throws Exception {
		SolrDocumentKafkaPublisher pub = new SolrDocumentKafkaPublisher();
		pub.runProducer();
//		pub.runShardProducer();
	}

	private void runShardProducer() throws Exception {
		final Producer<String, SolrDocument> producer = createProducer();
		long time = System.currentTimeMillis();
		final String[] ids = new String[] {"doc_4e1dd29c-85fc-4221-a25a-b84fcf24f4af",
				"doc_ff595762-b312-489a-8720-a8157855f781"};
		try {
			for (String id : ids) {
				SolrDocument doc = createSampleDocument(id);
				ProducerRecord<String, SolrDocument> record = new ProducerRecord<String, SolrDocument>(TOPIC, id, doc);
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

	private static Producer<String, SolrDocument> createProducer() {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVERS);
		props.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, SolrDocumentSerializer.class.getName());
		return new KafkaProducer<>(props);
	}

	public void runProducer() throws Exception {
		final Producer<String, SolrDocument> producer = createProducer();
		long time = System.currentTimeMillis();
		try {
			long i = 0;
			for (SolrDocument doc : docs) {
				ProducerRecord<String, SolrDocument> record =
						new ProducerRecord<String, SolrDocument>(TOPIC, doc.get("id").toString(), doc);
				// It's probably faster if we just send and don't bother with the get.  maybe just do it on the last one?
				// producer.send(record);
				producer.send(record).get();
				long elapsedTime = System.currentTimeMillis() - time;
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