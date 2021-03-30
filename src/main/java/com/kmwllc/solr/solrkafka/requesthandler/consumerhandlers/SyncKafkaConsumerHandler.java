package com.kmwllc.solr.solrkafka.requesthandler.consumerhandlers;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Properties;

public class SyncKafkaConsumerHandler extends KafkaConsumerHandler {
  private static final Logger log = LogManager.getLogger(SyncKafkaConsumerHandler.class);

  SyncKafkaConsumerHandler(Properties consumerProps, String topic, boolean fromBeginning, boolean readFullyAndExit) {
    // TODO: how large should we make the queue size?
    super(consumerProps, topic, fromBeginning, readFullyAndExit, Integer.MAX_VALUE);
    running = true;
  }

  @Override
  public void stop() {
    running = false;
    try {
      // TODO: something better than just waiting for the previous poll attempt to finish.
      Thread.sleep(pollTimeout + 1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    consumer.close();
  }

  @Override
  public void commitOffsets(Map<TopicPartition, OffsetAndMetadata> commit) {
    acquireSemaphore();
    commitToConsumer(commit);
    consumerSemaphore.release();
  }

  @Override
  public boolean hasNext() {
    while (running && inputQueue.size() == 0) {
      loadSolrDocs();
    }
    return inputQueue.size() > 0;
  }
}
