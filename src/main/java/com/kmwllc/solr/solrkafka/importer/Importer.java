package com.kmwllc.solr.solrkafka.importer;

import org.apache.solr.core.SolrCore;

import java.util.Map;

public interface Importer {
  void startThread();

  boolean isThreadAlive();

  void stop();

  /**
   * Get the number of messages this {@code Importer} is behind the {@link org.apache.kafka.clients.consumer.Consumer}.
   *
   * @return A map from partition names to offsets
   */
  Map<String, Long> getConsumerGroupLag();

  void pause();

  void resume();

  /**
   * Starts the {@link org.apache.kafka.clients.consumer.Consumer} from the beginning.
   */
  void rewind();

  void setNewCore(SolrCore core);

  Status getStatus();
}
