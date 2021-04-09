package com.kmwllc.solr.solrkafka.handler.request;

import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.mime.MIME;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.JavaBinCodec;
import org.slf4j.Logger;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.SolrCmdDistributor;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

public class CustomCommandDistributor {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public void distribAdd(AddUpdateCommand cmd, List<SolrCmdDistributor.Node> nodes, ModifiableSolrParams params) throws IOException {
    for (SolrCmdDistributor.Node node : nodes) {
      try (CloseableHttpClient client = HttpClients.createDefault();
           JavaBinCodec codec = new JavaBinCodec();
           ByteArrayOutputStream os = new ByteArrayOutputStream()) {
        SolrInputDocument doc = cmd.solrDoc;
        codec.marshal(doc, os);
        URI uri = new URIBuilder(node.getUrl() + "kafka/distrib")
            .setParameter("docId", cmd.getIndexedIdStr()).build();
        HttpPost post = new HttpPost(uri);
        post.setEntity(new ByteArrayEntity(os.toByteArray()));
        post.setHeader(MIME.CONTENT_TYPE, "application/octet-stream");
        client.execute(post);
      } catch (URISyntaxException e) {
        log.error("Invalid URI created for solr command distribution", e);
      }
    }
  }
}
