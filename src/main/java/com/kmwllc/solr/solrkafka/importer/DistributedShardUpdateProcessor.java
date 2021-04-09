package com.kmwllc.solr.solrkafka.importer;

import com.kmwllc.solr.solrkafka.handler.request.CustomCommandDistributor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.cloud.ZkShardTerms;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZooKeeperException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.SolrCmdDistributor;
import org.apache.solr.update.UpdateCommand;
import org.apache.solr.update.processor.DistributedZkUpdateProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessorFactory;
import org.apache.solr.util.TestInjection;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.solr.update.processor.DistributingUpdateProcessorFactory.DISTRIB_UPDATE_PARAM;

public class DistributedShardUpdateProcessor extends DistributedZkUpdateProcessor {
  private static final Logger log = LogManager.getLogger(DistributedShardUpdateProcessor.class);
  private final boolean ignoreShardRouting;
  private final CloudDescriptor cloudDesc;
  private final ZkController zkController;
  private final String collection;
  private Set<String> skippedCoreNodeNames;
  private final CustomCommandDistributor cmdDistributor;

  //used for keeping track of replicas that have processed an add/update from the leader
  private RollupRequestReplicationTracker rollupReplicationTracker = null;
  private LeaderRequestReplicationTracker leaderReplicationTracker = null;

  public DistributedShardUpdateProcessor(SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next, boolean ignoreShardRouting) {
    super(req, rsp, next);
    CoreContainer cc = req.getCore().getCoreContainer();
    cloudDesc = req.getCore().getCoreDescriptor().getCloudDescriptor();
    zkController = cc.getZkController();
    collection = cloudDesc.getCollectionName();

    cmdDistributor = new CustomCommandDistributor();
    this.ignoreShardRouting = ignoreShardRouting;
  }

  public static class DistributedShardUpdateProcessorFactory extends UpdateRequestProcessorFactory {
    private final boolean ignoreRouting;

    public DistributedShardUpdateProcessorFactory(boolean ignoreRouting) {
      this.ignoreRouting = ignoreRouting;
    }

    @Override
    public UpdateRequestProcessor getInstance(SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
      return new DistributedShardUpdateProcessor(req, rsp, next, ignoreRouting);
    }
  }

  public boolean isIgnoreShardRouting() {
    return ignoreShardRouting;
  }

  @Override
  protected List<SolrCmdDistributor.Node> setupRequest(String id, SolrInputDocument doc, String route) {

    if (!ignoreShardRouting) {
      return super.setupRequest(id, doc);
    }

    if ((updateCommand.getFlags() & (UpdateCommand.REPLAY | UpdateCommand.PEER_SYNC)) != 0) {
      isLeader = false;     // we actually might be the leader, but we don't want leader-logic for these types of updates anyway.
      forwardToLeader = false;
      return null;
    }

    clusterState = zkController.getClusterState();
    DocCollection coll = clusterState.getCollection(collection);
    Collection<Slice> slices = coll.getSlices();
    String shardId = cloudDesc.getShardId();

    if (slices.isEmpty()) {
      slices = List.of(coll.getSlice(shardId));
      if (slices.stream().anyMatch(Objects::isNull)) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "No shard " + shardId + " in " + coll);
      }
    }

    DistribPhase phase =
        DistribPhase.parseParam(req.getParams().get(DISTRIB_UPDATE_PARAM));

    Slice mySlice = coll.getSlice(shardId);
    Slice.State state = mySlice.getState();

    if (DistribPhase.FROMLEADER == phase && !(state == Slice.State.CONSTRUCTION || state == Slice.State.RECOVERY)) {
      if (cloudDesc.isLeader()) {
        // locally we think we are leader but the request says it came FROMLEADER
        // that could indicate a problem, let the full logic below figure it out
      } else {

        assert TestInjection.injectFailReplicaRequests();

        isLeader = false;     // we actually might be the leader, but we don't want leader-logic for these types of updates anyway.
        forwardToLeader = false;
        return null;
      }
    }

    try {
      Map<String, Replica> leaders = new HashMap<>();
      String leaderReplicaName = "";
      for (Slice slice : slices) {
        Replica leaderReplica = zkController.getZkStateReader().getLeaderRetry(collection, slice.getName());
        isLeader = leaderReplica.getName().equals(cloudDesc.getCoreNodeName());
        if (!isLeader) {
          isSubShardLeader = amISubShardLeader(coll, slice, id, doc);
          if (isSubShardLeader) {
            shardId = cloudDesc.getShardId();
            leaderReplica = zkController.getZkStateReader().getLeaderRetry(collection, shardId);
          }
        }
        if (slice.equals(mySlice)) {
          leaderReplicaName = leaderReplica.getName();
        }
        leaders.put(leaderReplica.getName(), leaderReplica);
      }

      doDefensiveChecks();

      // if request is coming from another collection then we want it to be sent to all replicas
      // even if its phase is FROMLEADER
      String fromCollection = updateCommand.getReq().getParams().get(DISTRIB_FROM_COLLECTION);

      if (DistribPhase.FROMLEADER == phase && !isSubShardLeader && fromCollection == null) {
        // we are coming from the leader, just go local - add no urls
        forwardToLeader = false;
        return null;
      } else {
        // that means I want to forward onto my replicas...
        // so get the replicas...
        forwardToLeader = false;
        final String finalLeaderName = leaderReplicaName;
        // Get a Map of Shard IDs to the shard's replicas (minus this core's replica)
        Map<String, List<Replica>> replicas = clusterState.getCollection(collection)
            .getSlices().stream().collect(Collectors.toMap(Slice::getName,
                slice -> slice.getReplicas(EnumSet.of(Replica.Type.NRT, Replica.Type.TLOG)).stream()
                    .filter(replica -> !replica.getName().equals(finalLeaderName)).collect(Collectors.toList())));

        // If there are no replicas returned, exit
        if (replicas.isEmpty() || replicas.values().stream().allMatch(ls -> ls == null || ls.isEmpty())) {
          return null;
        }

        // check for test param that lets us miss replicas
        String[] skipList = req.getParams().getParams(TEST_DISTRIB_SKIP_SERVERS);
        Set<String> skipListSet = null;
        if (skipList != null) {
          skipListSet = new HashSet<>(skipList.length);
          skipListSet.addAll(Arrays.asList(skipList));
          log.info("test.distrib.skip.servers was found and contains:{}", skipListSet);
        }

        List<SolrCmdDistributor.Node> nodes = new ArrayList<>(replicas.values().stream().mapToInt(List::size).sum());
        skippedCoreNodeNames = new HashSet<>();
        for (String shard : replicas.keySet()) {
          ZkShardTerms zkShardTerms = zkController.getShardTerms(collection, shard);
          for (Replica replica : replicas.get(shard)) {
            String coreNodeName = replica.getName();
            if (skipList != null && skipListSet.contains(replica.getCoreUrl())) {
              if (log.isInfoEnabled()) {
                log.info("check url:{} against:{} result:true", replica.getCoreUrl(), skipListSet);
              }
            } else if (zkShardTerms.registered(coreNodeName) && zkShardTerms.skipSendingUpdatesTo(coreNodeName)) {
              if (log.isDebugEnabled()) {
                log.debug("skip url:{} cause its term is less than leader", replica.getCoreUrl());
              }
              skippedCoreNodeNames.add(replica.getName());
            } else if (!clusterState.getLiveNodes().contains(replica.getNodeName()) || replica.getState() == Replica.State.DOWN) {
              skippedCoreNodeNames.add(replica.getName());
            } else {
              nodes.add(new SolrCmdDistributor.StdNode(new ZkCoreNodeProps(replica), collection, shardId, maxRetriesToFollowers));
            }
          }
        }
        return nodes;
      }

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "", e);
    }
  }

  private void doDefensiveChecks() {
    String from = req.getParams().get(DISTRIB_FROM);

    boolean localIsLeader = cloudDesc.isLeader();

    int count = 0;
    while (((isLeader && !localIsLeader) || (isSubShardLeader && !localIsLeader)) && count < 5) {
      count++;
      // re-getting localIsLeader since we published to ZK first before setting localIsLeader value
      localIsLeader = cloudDesc.isLeader();
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    if ((isLeader && !localIsLeader) || (isSubShardLeader && !localIsLeader)) {
      log.error("ClusterState says we are the leader, but locally we don't think so");
      throw new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE,
          "ClusterState says we are the leader (" + zkController.getBaseUrl()
              + "/" + req.getCore().getName() + "), but locally we don't think so. Request came from " + from);
    }
  }

  @Override
  protected void doDistribFinish() {
    if (!ignoreShardRouting) {
      super.doDistribFinish();
      return;
    }

    clusterState = zkController.getClusterState();

    boolean shouldUpdateTerms = isLeader && isIndexChanged;
    if (shouldUpdateTerms) {
      ZkShardTerms zkShardTerms = zkController.getShardTerms(cloudDesc.getCollectionName(), cloudDesc.getShardId());
      if (skippedCoreNodeNames != null) {
        zkShardTerms.ensureTermsIsHigher(cloudDesc.getCoreNodeName(), skippedCoreNodeNames);
      }
      zkController.getShardTerms(collection, cloudDesc.getShardId()).ensureHighestTermsAreNotZero();
    }

    super.doDistribFinish();
  }

  @Override
  protected void doDistribAdd(AddUpdateCommand cmd) throws IOException {
    if (!ignoreShardRouting) {
      super.doDistribAdd(cmd);
      return;
    }

    if (isLeader && !isSubShardLeader)  {
      DocCollection coll = clusterState.getCollection(collection);
      List<SolrCmdDistributor.Node> subShardLeaders = getSubShardLeaders(coll, cloudDesc.getShardId(), cmd.getIndexedIdStr(), cmd.getSolrInputDocument());
      // the list<node> will actually have only one element for an add request
      if (subShardLeaders != null && !subShardLeaders.isEmpty()) {
        ModifiableSolrParams params = new ModifiableSolrParams(filterParams(req.getParams()));
        params.set(DISTRIB_UPDATE_PARAM, DistribPhase.FROMLEADER.toString());
        params.set(DISTRIB_FROM, ZkCoreNodeProps.getCoreUrl(
            zkController.getBaseUrl(), req.getCore().getName()));
        params.set(DISTRIB_FROM_PARENT, cloudDesc.getShardId());
        cmdDistributor.distribAdd(cmd, subShardLeaders, params);
      }
      final List<SolrCmdDistributor.Node> nodesByRoutingRules = getNodesByRoutingRules(clusterState, coll, cmd.getIndexedIdStr(), cmd.getSolrInputDocument());
      if (nodesByRoutingRules != null && !nodesByRoutingRules.isEmpty())  {
        ModifiableSolrParams params = new ModifiableSolrParams(filterParams(req.getParams()));
        params.set(DISTRIB_UPDATE_PARAM, DistribPhase.FROMLEADER.toString());
        params.set(DISTRIB_FROM, ZkCoreNodeProps.getCoreUrl(
            zkController.getBaseUrl(), req.getCore().getName()));
        params.set(DISTRIB_FROM_COLLECTION, collection);
        params.set(DISTRIB_FROM_SHARD, cloudDesc.getShardId());
        cmdDistributor.distribAdd(cmd, nodesByRoutingRules, params);
      }
    }

    if (nodes != null) {
      ModifiableSolrParams params = new ModifiableSolrParams(filterParams(req.getParams()));
      params.set(DISTRIB_UPDATE_PARAM,
          (isLeader || isSubShardLeader ?
              DistribPhase.FROMLEADER.toString() :
              DistribPhase.TOLEADER.toString()));
      params.set(DISTRIB_FROM, ZkCoreNodeProps.getCoreUrl(
          zkController.getBaseUrl(), req.getCore().getName()));

      if (req.getParams().get(UpdateRequest.MIN_REPFACT) != null) {
        // TODO: Kept for rolling upgrades only. Should be removed in Solr 9
        params.set(UpdateRequest.MIN_REPFACT, req.getParams().get(UpdateRequest.MIN_REPFACT));
      }

      if (cmd.isInPlaceUpdate()) {
        params.set(DISTRIB_INPLACE_PREVVERSION, String.valueOf(cmd.prevVersion));

        // Use synchronous=true so that a new connection is used, instead
        // of the update being streamed through an existing streaming client.
        // When using a streaming client, the previous update
        // and the current in-place update (that depends on the previous update), if reordered
        // in the stream, can result in the current update being bottled up behind the previous
        // update in the stream and can lead to degraded performance.
        cmdDistributor.distribAdd(cmd, nodes, params);
      } else {
        cmdDistributor.distribAdd(cmd, nodes, params);
      }
    }
  }

  @Override
  public void processAdd(AddUpdateCommand cmd) throws IOException {
    if (!ignoreShardRouting) {
      super.processAdd(cmd);
      return;
    }

    checkReplicationTracker(cmd);
    super.processAdd(cmd);
  }

  private void checkReplicationTracker(UpdateCommand cmd) {
    SolrParams rp = cmd.getReq().getParams();
    String distribUpdate = rp.get(DISTRIB_UPDATE_PARAM);
    // Ok,we're receiving the original request, we need a rollup tracker, but only one so we accumulate over the
    // course of a batch.
    if ((distribUpdate == null || DistribPhase.NONE.toString().equals(distribUpdate)) &&
        rollupReplicationTracker == null) {
      rollupReplicationTracker = new RollupRequestReplicationTracker();
    }
    // If we're a leader, we need a leader replication tracker, so let's do that. If there are multiple docs in
    // a batch we need to use the _same_ leader replication tracker.
    if (isLeader && leaderReplicationTracker == null) {
      leaderReplicationTracker = new LeaderRequestReplicationTracker(
          req.getCore().getCoreDescriptor().getCloudDescriptor().getShardId());
    }
  }
}
