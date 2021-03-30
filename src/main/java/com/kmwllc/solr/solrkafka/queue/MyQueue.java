package com.kmwllc.solr.solrkafka.queue;

public interface MyQueue<T> {
  void put(T item) throws InterruptedException;

  T poll() throws InterruptedException;

  boolean isEmpty();
}
