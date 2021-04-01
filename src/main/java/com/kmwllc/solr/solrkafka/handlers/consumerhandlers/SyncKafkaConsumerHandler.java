package com.kmwllc.solr.solrkafka.handlers.consumerhandlers;

import com.kmwllc.solr.solrkafka.importers.Status;
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
    status = Status.RUNNING;
  }

  @Override
  public void stop() {
    log.info("Stopping consumer handler and closing consumer");
    status = Status.DONE;
    try {
      // TODO: something better than just waiting for the previous poll attempt to finish.
      Thread.sleep(POLL_TIMEOUT * 2);
    } catch (InterruptedException e) {
      log.error("Sleep for shutdown interrupted", e);
    }
    if (!isClosed) {
      consumer.close();
      isClosed = true;
    }
  }

  @Override
  public void commitOffsets(Map<TopicPartition, OffsetAndMetadata> commit) {
    log.info("Commit called, performing synchronously");
    commitToConsumer(commit);
  }

  @Override
  public boolean hasNext() {
    while (status.isOperational() && inputQueue.isEmpty()) {
      log.debug("Input queue is empty, attempting to load documents");
      loadSolrDocs();
    }
    return !inputQueue.isEmpty();
  }
}
