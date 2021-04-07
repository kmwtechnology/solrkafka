package com.kmwllc.solr.solrkafka.datatype.solr;

import java.io.IOException;
import java.lang.invoke.MethodHandles;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.util.JavaBinCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolrDocumentDeserializer implements Deserializer<SolrDocument> {
	
	private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
	
	@Override
	public SolrDocument deserialize(String topic, byte[] data) {
		// can't reuse this codec, create a new one.
		JavaBinCodec codec = new JavaBinCodec();
		try {
			SolrDocument o = (SolrDocument) codec.unmarshal(data);
			codec.close();
			return o;
		} catch (IOException e) {
			log.error("Error deserializing data", e);
			return null;
		}
	}

}
