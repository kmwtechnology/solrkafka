package com.kmwllc.solr.solrkafka.queue;

import com.kmwllc.solr.solrkafka.requesthandler.DocumentData;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class BlockingMyQueue<T> implements MyQueue<T> {
  private final LinkedBlockingQueue<T> queue;
  private final long timeout;

  public BlockingMyQueue(int size, long timeout) {
    queue = new LinkedBlockingQueue<>(size);
    this.timeout = timeout;
  }

  @Override
  public T poll() throws InterruptedException {
    return queue.poll(timeout, TimeUnit.MILLISECONDS);
  }

  @Override
  public void put(T item) throws InterruptedException {
    queue.put(item);
  }

  @Override
  public boolean isEmpty() {
    return queue.isEmpty();
  }
}
