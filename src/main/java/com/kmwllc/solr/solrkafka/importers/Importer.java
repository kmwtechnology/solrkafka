package com.kmwllc.solr.solrkafka.importers;

public interface Importer {
  void startThread();

  boolean isThreadAlive();

  void stop();
}
