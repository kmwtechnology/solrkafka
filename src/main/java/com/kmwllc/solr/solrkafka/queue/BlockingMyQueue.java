package com.kmwllc.solr.solrkafka.queue;

import com.kmwllc.solr.solrkafka.requesthandler.DocumentData;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class BlockingMyQueue implements MyQueue {
  private final LinkedBlockingQueue<DocumentData> queue;
  private final int timeout;

  public BlockingMyQueue(int size, int timeout) {
    queue = new LinkedBlockingQueue<>(size);
    this.timeout = timeout;
  }

  @Override
  public DocumentData get() throws InterruptedException {
    return queue.poll(timeout, TimeUnit.MILLISECONDS);
  }

  @Override
  public void put(DocumentData item) {
    try {
    queue.put(item);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
