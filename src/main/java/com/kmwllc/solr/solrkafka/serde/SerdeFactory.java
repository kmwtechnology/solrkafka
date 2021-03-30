package com.kmwllc.solr.solrkafka.serde;

import com.kmwllc.solr.solrkafka.serde.json.JsonDeserializer;
import com.kmwllc.solr.solrkafka.serde.json.JsonSerializer;
import com.kmwllc.solr.solrkafka.serde.solr.SolrDocumentDeserializer;
import com.kmwllc.solr.solrkafka.serde.solr.SolrDocumentSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

public class SerdeFactory {

  public static Class<? extends Deserializer<?>> getDeserializer(String name) {
    if (name.equalsIgnoreCase("json")) {
      return JsonDeserializer.class;
    } else if (name.equalsIgnoreCase("solr")) {
      return SolrDocumentDeserializer.class;
    }
    throw new IllegalArgumentException("Unknown deserializer type");
  }

  public static Class<? extends Serializer<?>> getSerializer(String name) {
    if (name.equalsIgnoreCase("json")) {
      return JsonSerializer.class;
    } else if (name.equalsIgnoreCase("solr")) {
      return SolrDocumentSerializer.class;
    }
    throw new IllegalArgumentException("Unknown serializer type");
  }
}
