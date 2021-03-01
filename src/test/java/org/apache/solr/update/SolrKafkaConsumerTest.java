package org.apache.solr.update;

import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.store.Directory;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrEventListener;
import org.apache.solr.index.TieredMergePolicyFactory;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.DirectUpdateHandler2;
import org.apache.solr.update.UpdateHandler;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.params.CommonParams.VERSION_FIELD;

/**
 * 
 *
 */
public class SolrKafkaConsumerTest extends SolrTestCaseJ4 {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static String savedFactory;
  @BeforeClass
  public static void beforeClass() throws Exception {
    savedFactory = System.getProperty("solr.DirectoryFactory");
    System.setProperty("solr.directoryFactory", "org.apache.solr.core.MockFSDirectoryFactory");
    System.setProperty("enable.update.log", "false"); // schema12 doesn't support _version_
    systemSetPropertySolrTestsMergePolicyFactory(TieredMergePolicyFactory.class.getName());
    initCore("solrconfig.xml", "managed-schema");
  }
  
  @AfterClass
  public static void afterClass() {
    systemClearPropertySolrTestsMergePolicyFactory();
    if (savedFactory == null) {
      System.clearProperty("solr.directoryFactory");
    } else {
      System.setProperty("solr.directoryFactory", savedFactory);
    }
  }

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    clearIndex();
    assertU(commit());
  }


  @Test
  public void testBasics() throws Exception {

    // get initial metrics
    Map<String, Metric> metrics = h.getCoreContainer().getMetricManager()
        .registry(h.getCore().getCoreMetricManager().getRegistryName()).getMetrics();

    String PREFIX = "UPDATE.updateHandler.";

    String commitsName = PREFIX + "commits";
    assertTrue(metrics.containsKey(commitsName));
    String addsName = PREFIX + "adds";
    assertTrue(metrics.containsKey(addsName));
    String cumulativeAddsName = PREFIX + "cumulativeAdds";
    String delsIName = PREFIX + "deletesById";
    String cumulativeDelsIName = PREFIX + "cumulativeDeletesById";
    String delsQName = PREFIX + "deletesByQuery";
    String cumulativeDelsQName = PREFIX + "cumulativeDeletesByQuery";
    long commits = ((Meter) metrics.get(commitsName)).getCount();
    long adds = ((Gauge<Number>) metrics.get(addsName)).getValue().longValue();
    long cumulativeAdds = ((Meter) metrics.get(cumulativeAddsName)).getCount();
    long cumulativeDelsI = ((Meter) metrics.get(cumulativeDelsIName)).getCount();
    long cumulativeDelsQ = ((Meter) metrics.get(cumulativeDelsQName)).getCount();


    assertNull("This test requires a schema that has no version field, " +
               "it appears the schema file in use has been edited to violate " +
               "this requirement",
               h.getCore().getLatestSchema().getFieldOrNull(VERSION_FIELD));

    assertU(adoc("id","5"));
    assertU(adoc("id","6"));

    // search - not committed - docs should not be found.
    assertQ(req("q","id:5"), "//*[@numFound='0']");
    assertQ(req("q","id:6"), "//*[@numFound='0']");

    long newAdds = ((Gauge<Number>) metrics.get(addsName)).getValue().longValue();
    long newCumulativeAdds = ((Meter) metrics.get(cumulativeAddsName)).getCount();
    assertEquals("new adds", 2, newAdds - adds);
    assertEquals("new cumulative adds", 2, newCumulativeAdds - cumulativeAdds);

    assertU(commit());

    long newCommits = ((Meter) metrics.get(commitsName)).getCount();
    assertEquals("new commits", 1, newCommits - commits);

    newAdds = ((Gauge<Number>) metrics.get(addsName)).getValue().longValue();
    newCumulativeAdds = ((Meter) metrics.get(cumulativeAddsName)).getCount();
    // adds should be reset to 0 after commit
    assertEquals("new adds after commit", 0, newAdds);
    // not so with cumulative ones!
    assertEquals("new cumulative adds after commit", 2, newCumulativeAdds - cumulativeAdds);

    // now they should be there
    assertQ(req("q","id:5"), "//*[@numFound='1']");
    assertQ(req("q","id:6"), "//*[@numFound='1']");

    // now delete one
    assertU(delI("5"));

    long newDelsI = ((Gauge<Number>) metrics.get(delsIName)).getValue().longValue();
    long newCumulativeDelsI = ((Meter) metrics.get(cumulativeDelsIName)).getCount();
    assertEquals("new delsI", 1, newDelsI);
    assertEquals("new cumulative delsI", 1, newCumulativeDelsI - cumulativeDelsI);

    // not committed yet
    assertQ(req("q","id:5"), "//*[@numFound='1']");

    assertU(commit());
    // delsI should be reset to 0 after commit
    newDelsI = ((Gauge<Number>) metrics.get(delsIName)).getValue().longValue();
    newCumulativeDelsI = ((Meter) metrics.get(cumulativeDelsIName)).getCount();
    assertEquals("new delsI after commit", 0, newDelsI);
    assertEquals("new cumulative delsI after commit", 1, newCumulativeDelsI - cumulativeDelsI);

    // 5 should be gone
    assertQ(req("q","id:5"), "//*[@numFound='0']");
    assertQ(req("q","id:6"), "//*[@numFound='1']");

    // now delete all
    assertU(delQ("*:*"));

    long newDelsQ = ((Gauge<Number>) metrics.get(delsQName)).getValue().longValue();
    long newCumulativeDelsQ = ((Meter) metrics.get(cumulativeDelsQName)).getCount();
    assertEquals("new delsQ", 1, newDelsQ);
    assertEquals("new cumulative delsQ", 1, newCumulativeDelsQ - cumulativeDelsQ);

    // not committed yet
    assertQ(req("q","id:6"), "//*[@numFound='1']");

    assertU(commit());

    newDelsQ = ((Gauge<Number>) metrics.get(delsQName)).getValue().longValue();
    newCumulativeDelsQ = ((Meter) metrics.get(cumulativeDelsQName)).getCount();
    assertEquals("new delsQ after commit", 0, newDelsQ);
    assertEquals("new cumulative delsQ after commit", 1, newCumulativeDelsQ - cumulativeDelsQ);

    // 6 should be gone
    assertQ(req("q","id:6"), "//*[@numFound='0']");

    // verify final metrics
    newCommits = ((Meter) metrics.get(commitsName)).getCount();
    assertEquals("new commits", 3, newCommits - commits);
    newAdds = ((Gauge<Number>) metrics.get(addsName)).getValue().longValue();
    assertEquals("new adds", 0, newAdds);
    newCumulativeAdds = ((Meter) metrics.get(cumulativeAddsName)).getCount();
    assertEquals("new cumulative adds", 2, newCumulativeAdds - cumulativeAdds);
    newDelsI = ((Gauge<Number>) metrics.get(delsIName)).getValue().longValue();
    assertEquals("new delsI", 0, newDelsI);
    newCumulativeDelsI = ((Meter) metrics.get(cumulativeDelsIName)).getCount();
    assertEquals("new cumulative delsI", 1, newCumulativeDelsI - cumulativeDelsI);

  }

  static class MySolrEventListener implements SolrEventListener {
    AtomicInteger newSearcherCount = new AtomicInteger(0);
    AtomicLong newSearcherOpenedAt = new AtomicLong(Long.MAX_VALUE);
    AtomicLong postSoftCommitAt = new AtomicLong(Long.MAX_VALUE);

    @Override
    public void postCommit() {
    }

    @Override
    public void postSoftCommit() {
      postSoftCommitAt.set(System.nanoTime());
    }

    @Override
    public void newSearcher(SolrIndexSearcher newSearcher, SolrIndexSearcher currentSearcher) {
      newSearcherCount.incrementAndGet();
      newSearcherOpenedAt.set(newSearcher.getOpenNanoTime());
    }

    @Override
    public void init(NamedList args) {

    }
  }
}
