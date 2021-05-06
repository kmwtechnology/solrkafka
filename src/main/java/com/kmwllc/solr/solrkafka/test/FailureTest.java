package com.kmwllc.solr.solrkafka.test;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class FailureTest {
  private static final Logger log = LogManager.getLogger(FailureTest.class);

  public static void main(String[] args) throws InterruptedException {
    log.info("Sleeping for 15 seconds before throwing exception");

    Thread.sleep(15000);
    log.error("Error", new IllegalStateException("This is expected"));
    System.exit(1);
  }
}
