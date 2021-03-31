package com.kmwllc.solr.solrkafka.handlers.consumerhandlers;

import com.kmwllc.solr.solrkafka.queue.NonBlockingMyQueue;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Properties;

public class SyncKafkaConsumerHandler extends KafkaConsumerHandler {
  private static final Logger log = LogManager.getLogger(SyncKafkaConsumerHandler.class);

  SyncKafkaConsumerHandler(Properties consumerProps, String topic, boolean fromBeginning, boolean readFullyAndExit,
                           String dataType) {
    super(consumerProps, topic, fromBeginning, readFullyAndExit, new NonBlockingMyQueue<>(), dataType);
    running = true;
  }

  @Override
  public void stop() {
    running = false;
    try {
      // TODO: something better than just waiting for the previous poll attempt to finish.
      Thread.sleep(POLL_TIMEOUT + 1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    if (!isClosed) {
      consumer.close();
      isClosed = true;
    }
  }

  @Override
  public void commitOffsets(Map<TopicPartition, OffsetAndMetadata> commit) {
    commitToConsumer(commit);
  }

  @Override
  public boolean hasNext() {
    while (running && inputQueue.isEmpty()) {
      loadSolrDocs();
    }
    return !inputQueue.isEmpty();
  }
}
