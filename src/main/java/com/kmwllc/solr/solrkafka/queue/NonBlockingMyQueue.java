package com.kmwllc.solr.solrkafka.queue;

import java.util.ArrayDeque;

public class NonBlockingMyQueue<T> implements MyQueue<T> {
  private final ArrayDeque<T> queue = new ArrayDeque<>();

  @Override
  public void put(T item) {
    queue.add(item);
  }

  @Override
  public T poll() throws InterruptedException {
    return queue.remove();
  }

  @Override
  public boolean isEmpty() {
    return queue.isEmpty();
  }
}
