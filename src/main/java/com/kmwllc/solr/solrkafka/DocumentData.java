package com.kmwllc.solr.solrkafka;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;

public class DocumentData {
  private final SolrDocument doc;
  private final TopicPartition part;
  private final OffsetAndMetadata offset;

  public DocumentData(SolrDocument doc, TopicPartition part, OffsetAndMetadata offset) {
    this.doc = doc;
    this.part = part;
    this.offset = offset;
  }

  public OffsetAndMetadata getOffset() {
    return offset;
  }

  public SolrDocument getDoc() {
    return doc;
  }

  public TopicPartition getPart() {
    return part;
  }

  public SolrInputDocument convertToInputDoc() {
    SolrInputDocument inDoc = new SolrInputDocument();

    for (String name : doc.getFieldNames()) {
      inDoc.addField(name, doc.getFieldValue(name));
    }

    return inDoc;
  }
}
