package com.kmwllc.solr.solrkafka.requesthandler;

import com.kmwllc.solr.solrkafka.serde.SolrDocumentDeserializer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrEventListener;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.security.AuthorizationContext;
import org.apache.solr.security.PermissionNameProvider;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.SolrIndexWriter;
import org.apache.solr.update.UpdateHandler;
import org.apache.solr.util.circuitbreaker.CircuitBreaker;
import org.apache.solr.util.circuitbreaker.CircuitBreakerManager;
import org.apache.solr.util.plugin.PluginInfoInitialized;
import org.apache.solr.util.plugin.SolrCoreAware;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;

public class SolrKafkaRequestHandler extends RequestHandlerBase implements SolrCoreAware, PluginInfoInitialized, PermissionNameProvider, Runnable, AutoCloseable {
  private static final Logger log = LogManager.getLogger(SolrKafkaRequestHandler.class);
  private SolrCore core;
  private Thread thread;
  private LinkedBlockingQueue<DocumentData> queue;
  public boolean fromBeginning;
  public boolean readFullyAndExit = false;
  private static final String topic = "testtopic";
  private volatile KafkaHandler iter;
  private final Properties initProps = new Properties();

  public SolrKafkaRequestHandler() {
    int queueSize = 100;
    queue = new LinkedBlockingQueue<>(queueSize);
    log.info("Kafka Consumer created.");
  }

  private Consumer<String, SolrDocument> createConsumer(Properties props) {
    String bootStrapServers = "localhost:9092";
    props.putIfAbsent(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
    String consumerGroupId = "SolrKafkaConsumer";
    props.putIfAbsent(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
    props.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SolrDocumentDeserializer.class.getName());
    // How do we force the offset ?
    props.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    // https://www.programmersought.com/article/37481195666/
    // https://stackoverflow.com/questions/1771679/difference-between-threads-context-class-loader-and-normal-classloader/36228195#36228195
    // Override class loader for creating KafkaConsumer
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(null);
    // Create the consumer using props.
    KafkaConsumer<String, SolrDocument> consumer = new KafkaConsumer<>(props);
    Thread.currentThread().setContextClassLoader(loader);
    return consumer;
  }

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    SolrKafkaStatusRequestHandler.setHandler(this);
    ResponseBuilder rb = new ResponseBuilder(req, rsp, new ArrayList<>());

    CircuitBreakerManager circuitBreakerManager = req.getCore().getCircuitBreakerManager();
    List<CircuitBreaker> breakers = circuitBreakerManager.checkTripped();
    if (breakers != null) {
      String errorMsg = CircuitBreakerManager.toErrorMessage(breakers);
      rsp.add(CommonParams.STATUS, CommonParams.FAILURE);
      rsp.setException(new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE, "Circuit Breakers tripped " + errorMsg));
      return;
    }

    if (thread != null && thread.isAlive()) {
      rsp.add("Status", "Request already running");
      return;
    }

    fromBeginning = req.getParams().getBool("fromBeginning", false);
    readFullyAndExit = req.getParams().getBool("exitAtEnd", readFullyAndExit);
    thread = new Thread(this);
    thread.start();
    rsp.add("Status", "Started");
  }

  @Override
  public String getDescription() {
    return "Loading documents from Kafka";
  }

  @Override
  public Boolean registerV2() {
    return Boolean.TRUE;
  }

  @Override
  public Name getPermissionName(AuthorizationContext request) {
    return Name.READ_PERM;
  }

  @Override
  public void init(PluginInfo info) {
    init(info.initArgs);
  }

  @Override
  public void inform(SolrCore core) {
    this.core = core;
  }

  private SolrIndexWriter createWriter() {
    try {
      // TODO: create or open index?
      // probably open since it will be created by the core
      return SolrIndexWriter.create(core, core.getName(), core.getNewIndexDir(), core.getDirectoryFactory(),
          false, core.getLatestSchema(), core.getSolrConfig().indexConfig, core.getDeletionPolicy(), core.getCodec());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public void close() {
    iter.stop();
    thread.interrupt();
  }

  @Override
  public void run() {
    log.info("Starting Kafka consumer");
    UpdateHandler updateHandler = core.getUpdateHandler();
    Consumer<String, SolrDocument> consumer = createConsumer(initProps);
    // Subscribe to the topic.
    consumer.subscribe(Collections.singletonList(topic));
    // If we are supposed to start from the beginning.. let's see if we can seek there.
    if (fromBeginning) {
      consumer.poll(0);
      consumer.seekToBeginning(consumer.assignment());
    }
    iter = new KafkaHandler(consumer, queue);
    iter.readFullyAndExit = this.readFullyAndExit;
    // TODO: would this be better as an atoimc reference?
    Semaphore mapLock = new Semaphore(1);
    Map<TopicPartition, OffsetAndMetadata> addedOffsets = new HashMap<>();

    updateHandler.registerCommitCallback(new SolrEventListener() {
      @Override
      public void postCommit() {
        try {
          mapLock.acquire();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }

        iter.commitIndex(Map.copyOf(addedOffsets));
        addedOffsets.clear();
        mapLock.release();
        log.info("Updated offsets");
      }

      @Override
      public void postSoftCommit() { }

      @Override
      public void newSearcher(SolrIndexSearcher newSearcher, SolrIndexSearcher currentSearcher) { }

      @Override
      public void init(NamedList args) { }
    });

    while (iter.hasNext()) {
      DocumentData doc = iter.next();
      AddUpdateCommand add = new AddUpdateCommand(buildReq(doc.getDoc()));
      add.solrDoc = doc.convertToInputDoc();
      try {
        updateHandler.addDoc(add);
        try {
          mapLock.acquire();
        } catch (InterruptedException e) {
          thread.interrupt();
        }
        addedOffsets.put(doc.getPart(), doc.getOffset());
        mapLock.release();
      } catch (IOException | SolrException e) {
        log.error("Error occurred with Kafka index handler", e);
        return;
      }
    }

    log.info("Kafka consumer finished");
    iter.stop();
  }

  private SolrQueryRequest buildReq(Map<String, Object> doc) {
    Map.Entry<String, String>[] nl = new NamedList.NamedListEntry[3];
    nl[0] = new NamedList.NamedListEntry<>("commitWithin", "1000");
    nl[1] = new NamedList.NamedListEntry<>("overwrite", "true");
    nl[2] = new NamedList.NamedListEntry<>("wt", "json");
    SolrQueryRequest req = new LocalSolrQueryRequest(core, new NamedList<>(nl));
    req.setJSON(doc);
    return req;
  }

  boolean isThreadAlive() {
    return thread != null && thread.isAlive();
  }
}
