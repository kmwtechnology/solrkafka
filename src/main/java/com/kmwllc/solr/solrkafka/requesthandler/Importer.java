package com.kmwllc.solr.solrkafka.requesthandler;

public interface Importer {
  void startThread();

  boolean isThreadAlive();

  void stop();
}
