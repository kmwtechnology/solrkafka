package com.kmwllc.solr.solrkafka.importers;

import java.util.Map;

public interface Importer {
  void startThread();

  boolean isThreadAlive();

  void stop();

  Map<String, Long> getConsumerGroupLag();

//  void pause();
}
