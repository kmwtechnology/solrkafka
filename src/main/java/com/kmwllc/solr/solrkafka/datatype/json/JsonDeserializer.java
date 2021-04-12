package com.kmwllc.solr.solrkafka.datatype.json;


import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.common.SolrDocument;

import java.io.IOException;
import java.util.Map;

public class JsonDeserializer implements Deserializer<SolrDocument> {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Logger log = LogManager.getLogger(JsonDeserializer.class);

  @Override
  public SolrDocument deserialize(String topic, byte[] data) {
    try {
      Map<String, Object> map = MAPPER.readValue(data, new TypeReference<Map<String, Object>>() {});
      return new SolrDocument(map);
    } catch (IOException e) {
      log.error("Error deserializing JSON", e);
      return null;
    }
  }
}
