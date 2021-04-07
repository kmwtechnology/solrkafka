package com.kmwllc.solr.solrkafka.datatype.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class JsonSerializer implements Serializer<Map<String, Object>> {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Logger log = LogManager.getLogger(JsonSerializer.class);

  @Override
  public byte[] serialize(String topic, Map<String, Object> data) {
    try {
      return MAPPER.writeValueAsString(data).getBytes(StandardCharsets.UTF_8);
    } catch (IOException e) {
      log.error("Error occurred during serialization of JSON", e);
      return null;
    }
  }
}
