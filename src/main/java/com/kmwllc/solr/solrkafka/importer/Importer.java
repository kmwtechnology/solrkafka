package com.kmwllc.solr.solrkafka.importer;

import org.apache.solr.core.SolrCore;

import java.util.Map;

public interface Importer {
  void startThread();

  boolean isThreadAlive();

  void stop();

  Map<String, Long> getConsumerGroupLag();

  void pause();

  void resume();

  void rewind();

  void setNewCore(SolrCore core);

  Status getStatus();
}
