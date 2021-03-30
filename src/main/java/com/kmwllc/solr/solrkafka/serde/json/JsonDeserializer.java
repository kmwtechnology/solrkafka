package com.kmwllc.solr.solrkafka.serde.json;


import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Map;

public class JsonDeserializer implements Deserializer<Map<String, Object>> {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Logger log = LogManager.getLogger(JsonDeserializer.class);

  @Override
  public Map<String, Object> deserialize(String topic, byte[] data) {
    try {
      return MAPPER.readValue(data, new TypeReference<Map<String, Object>>() {});
    } catch (IOException e) {
      log.error("Error deserializing JSON", e);
      return null;
    }
  }
}
