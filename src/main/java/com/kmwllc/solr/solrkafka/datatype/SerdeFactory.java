package com.kmwllc.solr.solrkafka.datatype;

import com.kmwllc.solr.solrkafka.datatype.json.JsonDeserializer;
import com.kmwllc.solr.solrkafka.datatype.json.JsonSerializer;
import com.kmwllc.solr.solrkafka.datatype.solr.SolrDocumentDeserializer;
import com.kmwllc.solr.solrkafka.datatype.solr.SolrDocumentSerializer;
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
