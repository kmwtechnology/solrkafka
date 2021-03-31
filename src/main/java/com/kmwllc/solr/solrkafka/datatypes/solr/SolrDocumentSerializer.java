package com.kmwllc.solr.solrkafka.datatypes.solr;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandles;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.solr.common.SolrDocument;

import org.apache.solr.common.util.JavaBinCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolrDocumentSerializer implements Serializer<SolrDocument> {
	
	private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
	
	@Override
	public byte[] serialize(String topic, SolrDocument data) {
		// create the codec JavaBinCodec can't be reused so we create a new one.
		JavaBinCodec codec = new JavaBinCodec();
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try {
			codec.marshal(data, baos);
			codec.close();
			baos.close();
			return baos.toByteArray();
		} catch (IOException e) {
			log.error("Error serializing document {}", data, e);
			return null;
		}
	}

}
