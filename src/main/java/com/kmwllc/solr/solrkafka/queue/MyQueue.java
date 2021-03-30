package com.kmwllc.solr.solrkafka.queue;

import com.kmwllc.solr.solrkafka.requesthandler.DocumentData;

public interface MyQueue {
  void put(DocumentData item);

  DocumentData get() throws InterruptedException;
}
