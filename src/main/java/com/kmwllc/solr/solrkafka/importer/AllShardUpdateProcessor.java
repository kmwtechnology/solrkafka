package com.kmwllc.solr.solrkafka.importer;

import com.kmwllc.solr.solrkafka.handler.request.CustomCommandDistributor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.cloud.ZkShardTerms;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.*;
import org.apache.solr.common.params.ModifiableSolrParams;
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
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.solr.update.processor.DistributingUpdateProcessorFactory.DISTRIB_UPDATE_PARAM;

/**
 * A {@link org.apache.solr.update.processor.DistributedUpdateProcessor} that overrides default operations in the
 * {@link DistributedZkUpdateProcessor} to ensure a document is indexed on all shards. Should only be used in cases
 * where documents are expected to be indexed on all shards.
 * <p>
 * Most implementations contained in this class are copied from the {@link DistributedZkUpdateProcessor}. Minor
 * alterations have been made to distribute to all nodes.
 */
public class AllShardUpdateProcessor extends DistributedZkUpdateProcessor {
  private static final Logger log = LogManager.getLogger(AllShardUpdateProcessor.class);
  private final CloudDescriptor cloudDesc;
  private final ZkController zkController;
  private final String collection;
  private Set<String> skippedCoreNodeNames;
  private final CustomCommandDistributor cmdDistributor;
  private final Slice mySlice;
  private final String leaderReplicaName;
  private final String shardId;
  private final DocCollection coll;

  /**
   * @param req A {@link SolrQueryRequest}
   * @param rsp A {@link SolrQueryResponse}
   * @param next The next {@link UpdateRequestProcessor}, or {@code null} if none
   */
  public AllShardUpdateProcessor(SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
    super(req, rsp, next);
    CoreContainer cc = req.getCore().getCoreContainer();
    cloudDesc = req.getCore().getCoreDescriptor().getCloudDescriptor();
    zkController = cc.getZkController();
    collection = cloudDesc.getCollectionName();

    cmdDistributor = new CustomCommandDistributor();

    try {
      clusterState = zkController.getClusterState();
      coll = clusterState.getCollection(collection);
      shardId = cloudDesc.getShardId();
      mySlice = coll.getSlice(shardId);
      // TODO: might need to move this back into prepareRequest()
      Replica leaderReplica = zkController.getZkStateReader().getLeaderRetry(collection, mySlice.getName());
      isLeader = leaderReplica.getName().equals(cloudDesc.getCoreNodeName());
      leaderReplicaName = leaderReplica.getName();
    } catch (InterruptedException e) {
      throw new IllegalStateException("Interrupted while getting shard info", e);
    }
  }

  /**
   * A Factory to create the {@link AllShardUpdateProcessor}.
   */
  public static class AllShardUpdateProcessorFactory extends UpdateRequestProcessorFactory {
    @Override
    public UpdateRequestProcessor getInstance(SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
      return new AllShardUpdateProcessor(req, rsp, next);
    }
  }

  /**
   * Most of this copied from {@link DistributedZkUpdateProcessor#setupRequest(String, SolrInputDocument, String)}.
   */
  @Override
  protected List<SolrCmdDistributor.Node> setupRequest(String id, SolrInputDocument doc, String route) {
    if ((updateCommand.getFlags() & (UpdateCommand.REPLAY | UpdateCommand.PEER_SYNC)) != 0) {
      isLeader = false;     // we actually might be the leader, but we don't want leader-logic for these types of updates anyway.
      forwardToLeader = false;
      return null;
    }

    Collection<Slice> slices = coll.getSlices();

    // Attempt to get collection shards if none were found initially
    if (slices.isEmpty()) {
      slices = List.of(coll.getSlice(shardId));
      if (slices.stream().anyMatch(Objects::isNull)) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "No shard " + shardId + " in " + coll);
      }
    }

    DistribPhase phase =
        DistribPhase.parseParam(req.getParams().get(DISTRIB_UPDATE_PARAM));

    // Get the state of the current core's shard
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

      doDefensiveChecks();

      // if request is coming from another collection then we want it to be sent to all replicas
      // even if its phase is FROMLEADER
      String fromCollection = updateCommand.getReq().getParams().get(DISTRIB_FROM_COLLECTION);

      // Do nothing if received from another leader (should never happen)
      if (DistribPhase.FROMLEADER == phase && !isSubShardLeader && fromCollection == null) {
        // we are coming from the leader, just go local - add no urls
        forwardToLeader = false;
        return null;
      } else {
        forwardToLeader = false;

        // Get a Map of Shard IDs to the shard's replicas (minus this core's replica)
        Map<String, List<Replica>> replicas = clusterState.getCollection(collection)
            .getSlices().stream().collect(Collectors.toMap(Slice::getName,
                slice -> slice.getReplicas(EnumSet.of(Replica.Type.NRT, Replica.Type.TLOG)).stream()
                    .filter(replica -> !replica.getName().equals(leaderReplicaName)).collect(Collectors.toList())));

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

        // Check to see if any nodes should be skipped because of configurations
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
  }

  /**
   * Make sure Zookeeper agrees with our expected status. Copied from bottom part of DistributedZkUpdateProcessor's
   * doDefensiveChecks method.
   */
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
    // Copied from DistributedZkUpdateProcessor

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
    // very different from DistributedZkUpdateProcessor because we don't care about limiting shards/subshards

    if (nodes != null) {
      ModifiableSolrParams params = new ModifiableSolrParams(filterParams(req.getParams()));
      params.set(DISTRIB_FROM, ZkCoreNodeProps.getCoreUrl(
          zkController.getBaseUrl(), req.getCore().getName()));

      cmdDistributor.distribAdd(cmd, nodes);
    }
  }
}
