package com.kmwllc.solr.solrkafka.datatypes;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;

import java.util.Map;

/**
 * A POJO for linking a {@link SolrDocument} with its partition and offset.
 */
public class DocumentData {
  private final Map<String, Object> doc;
  private final TopicPartition part;
  private final OffsetAndMetadata offset;

  public DocumentData(Map<String, Object> doc, TopicPartition part, OffsetAndMetadata offset) {
    this.doc = doc;
    this.part = part;
    this.offset = offset;
  }

  public OffsetAndMetadata getOffset() {
    return offset;
  }

  public Map<String, Object> getDoc() {
    return doc;
  }

  public TopicPartition getPart() {
    return part;
  }

  public SolrInputDocument convertToInputDoc() {
    SolrInputDocument inDoc = new SolrInputDocument();

    for (String name : doc.keySet()) {
      inDoc.addField(name, doc.get(name));
    }

    return inDoc;
  }

  public static SolrInputDocument convertToInputDoc(Map<String, Object> doc) {
    SolrInputDocument inDoc = new SolrInputDocument();

    for (String name : doc.keySet()) {
      inDoc.addField(name, doc.get(name));
    }

    return inDoc;
  }
}
