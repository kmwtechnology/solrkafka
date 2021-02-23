package org.apache.solr.update;

import java.util.Iterator;
import java.util.Map;

import org.junit.Ignore;
import org.junit.Test;

import com.kmwllc.solr.solrkafka.KafkaDataSource;

public class TestKakfaIterator {

	@Test
	public void testIterator() {
		KafkaDataSource ds = new KafkaDataSource();
		ds.fromBeginning = true;
		ds.readFullyAndExit = true;
		ds.init(null, null);
		Iterator<Map<String, Object>> iter = ds.getData("testtopic");
		while(iter.hasNext()) {
			Map<String, Object> data = iter.next();
			System.out.println("Fetched Data : " + data);
		}
	}
}
