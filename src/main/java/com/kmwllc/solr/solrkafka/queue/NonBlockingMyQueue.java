package com.kmwllc.solr.solrkafka.queue;

import com.kmwllc.solr.solrkafka.requesthandler.DocumentData;

import java.util.ArrayDeque;

public class NonBlockingMyQueue implements MyQueue {
  private final ArrayDeque<DocumentData> queue = new ArrayDeque<>();

  @Override
  public void put(DocumentData item) {
    queue.add(item);
  }

  @Override
  public DocumentData get() throws InterruptedException {
    return queue.remove();
  }
}
