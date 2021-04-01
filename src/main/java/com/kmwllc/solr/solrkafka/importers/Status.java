package com.kmwllc.solr.solrkafka.importers;

public enum Status {
  NOT_STARTED(true), RUNNING(true), PAUSED(true), DONE, ERROR;

  private final boolean operational;

  Status(boolean operational) {
    this.operational = operational;
  }

  Status() {
    operational = false;
  }

  public boolean isOperational() {
    return operational;
  }
}
