package com.kmwllc.solr.solrkafka.handler.request;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.LBHttpSolrClient;
import org.apache.solr.client.solrj.impl.LBSolrClient;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.XML;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.solr.common.params.ShardParams._ROUTE_;

public class CustomUpdateRequest extends AbstractUpdateRequest {

  public static final String REPFACT = "rf";
  /**
   *   @deprecated Solr now always includes in the response the {@link #REPFACT}, this parameter
   *   doesn't need to be explicitly set
   */
  @Deprecated // SOLR-14034
  public static final String MIN_REPFACT = "min_rf";
  public static final String VER = "ver";
  public static final String OVERWRITE = "ow";
  public static final String COMMIT_WITHIN = "cw";
  private Map<SolrInputDocument, Map<String,Object>> documents = null;
  private Iterator<SolrInputDocument> docIterator = null;

  private boolean isLastDocInBatch = false;

  public CustomUpdateRequest() {
    super(SolrRequest.METHOD.POST, "/kafka/distrib");
  }

  // ---------------------------------------------------------------------------
  // ---------------------------------------------------------------------------

  /**
   * clear the pending documents and delete commands
   */
  public void clear() {
    if (documents != null) {
      documents.clear();
    }
  }

  // ---------------------------------------------------------------------------
  // ---------------------------------------------------------------------------

  /**
   * Add a SolrInputDocument to this request
   *
   * @throws NullPointerException if the document is null
   */
  public CustomUpdateRequest add(final SolrInputDocument doc) {
    Objects.requireNonNull(doc, "Cannot add a null SolrInputDocument");
    if (documents == null) {
      documents = new LinkedHashMap<>();
    }
    documents.put(doc, null);
    return this;
  }

  public CustomUpdateRequest add(String... fields) {
    return add(new SolrInputDocument(fields));
  }

  /**
   * Add a SolrInputDocument to this request
   * @param doc the document
   * @param overwrite true if the document should overwrite existing docs with the same id
   * @throws NullPointerException if the document is null
   */
  public CustomUpdateRequest add(final SolrInputDocument doc, Boolean overwrite) {
    return add(doc, null, overwrite);
  }

  /**
   * Add a SolrInputDocument to this request
   * @param doc the document
   * @param commitWithin the time horizon by which the document should be committed (in ms)
   * @throws NullPointerException if the document is null
   */
  public CustomUpdateRequest add(final SolrInputDocument doc, Integer commitWithin) {
    return add(doc, commitWithin, null);
  }

  /**
   * Add a SolrInputDocument to this request
   * @param doc the document
   * @param commitWithin the time horizon by which the document should be committed (in ms)
   * @param overwrite true if the document should overwrite existing docs with the same id
   * @throws NullPointerException if the document is null
   */
  public CustomUpdateRequest add(final SolrInputDocument doc, Integer commitWithin, Boolean overwrite) {
    Objects.requireNonNull(doc, "Cannot add a null SolrInputDocument");
    if (documents == null) {
      documents = new LinkedHashMap<>();
    }
    Map<String,Object> params = new HashMap<>(2);
    if (commitWithin != null) params.put(COMMIT_WITHIN, commitWithin);
    if (overwrite != null) params.put(OVERWRITE, overwrite);

    documents.put(doc, params);

    return this;
  }

  /**
   * Add a collection of SolrInputDocuments to this request
   *
   * @throws NullPointerException if any of the documents in the collection are null
   */
  public CustomUpdateRequest add(final Collection<SolrInputDocument> docs) {
    if (documents == null) {
      documents = new LinkedHashMap<>();
    }
    for (SolrInputDocument doc : docs) {
      Objects.requireNonNull(doc, "Cannot add a null SolrInputDocument");
      documents.put(doc, null);
    }
    return this;
  }

  public CustomUpdateRequest withRoute(String route) {
    if (params == null)
      params = new ModifiableSolrParams();
    params.set(_ROUTE_, route);
    return this;
  }

  private interface ReqSupplier<T extends LBSolrClient.Req> {
    T get(@SuppressWarnings({"rawtypes"}) SolrRequest solrRequest, List<String> servers);
  }

  private <T extends LBSolrClient.Req> Map<String, T> getRoutes(DocRouter router,
                                                                DocCollection col, Map<String,List<String>> urlMap,
                                                                ModifiableSolrParams params, String idField,
                                                                CustomUpdateRequest.ReqSupplier<T> reqSupplier) {
    Map<String,T> routes = new HashMap<>();
    if (documents != null) {
      Set<Map.Entry<SolrInputDocument,Map<String,Object>>> entries = documents.entrySet();
      for (Map.Entry<SolrInputDocument,Map<String,Object>> entry : entries) {
        SolrInputDocument doc = entry.getKey();
        Object id = doc.getFieldValue(idField);
        if (id == null) {
          return null;
        }
        Map<String, List<String>> urls = urlMap.values().stream().filter(ls -> ls != null && !ls.isEmpty())
            .collect(Collectors.toMap(ls -> ls.get(0), ls -> ls));

        for (String leaderUrl : urls.keySet()) {
          T request = routes
              .get(leaderUrl);
          if (request == null) {
            CustomUpdateRequest updateRequest = new CustomUpdateRequest();
            updateRequest.setMethod(getMethod());
            updateRequest.setCommitWithin(getCommitWithin());
            updateRequest.setParams(params);
            updateRequest.setPath(getPath());
            updateRequest.setBasicAuthCredentials(getBasicAuthUser(), getBasicAuthPassword());
            updateRequest.setResponseParser(getResponseParser());
            request = reqSupplier.get(updateRequest, urls.get(leaderUrl));
            routes.put(leaderUrl, request);
          }
          CustomUpdateRequest urequest = (CustomUpdateRequest) request.getRequest();
          Map<String,Object> value = entry.getValue();
          Boolean ow = null;
          if (value != null) {
            ow = (Boolean) value.get(OVERWRITE);
          }
          if (ow != null) {
            urequest.add(doc, ow);
          } else {
            urequest.add(doc);
          }
        }
      }
    }
    return routes;
  }

  /**
   * @param router to route updates with
   * @param col DocCollection for the updates
   * @param urlMap of the cluster
   * @param params params to use
   * @param idField the id field
   * @return a Map of urls to requests
   */
  public Map<String, LBSolrClient.Req> getRoutesToCollection(DocRouter router,
                                                             DocCollection col, Map<String,List<String>> urlMap,
                                                             ModifiableSolrParams params, String idField) {
    return getRoutes(router, col, urlMap, params, idField, LBSolrClient.Req::new);
  }

  /**
   * @param router to route updates with
   * @param col DocCollection for the updates
   * @param urlMap of the cluster
   * @param params params to use
   * @param idField the id field
   * @return a Map of urls to requests
   * @deprecated since 8.0, uses {@link #getRoutesToCollection(DocRouter, DocCollection, Map, ModifiableSolrParams, String)} instead
   */
  @Deprecated
  public Map<String, LBHttpSolrClient.Req> getRoutes(DocRouter router,
                                                     DocCollection col, Map<String,List<String>> urlMap,
                                                     ModifiableSolrParams params, String idField) {
    return getRoutes(router, col, urlMap, params, idField, LBHttpSolrClient.Req::new);
  }

  public void setDocIterator(Iterator<SolrInputDocument> docIterator) {
    this.docIterator = docIterator;
  }

  // --------------------------------------------------------------------------
  // --------------------------------------------------------------------------

  @Override
  public Collection<ContentStream> getContentStreams() throws IOException {
    return ClientUtils.toContentStreams(getXML(), ClientUtils.TEXT_XML);
  }

  public String getXML() throws IOException {
    StringWriter writer = new StringWriter();
    writeXML(writer);
    writer.flush();

    // If action is COMMIT or OPTIMIZE, it is sent with params
    String xml = writer.toString();
    // System.out.println( "SEND:"+xml );
    return (xml.length() > 0) ? xml : null;
  }

  private List<Map<SolrInputDocument,Map<String,Object>>> getDocLists(Map<SolrInputDocument,Map<String,Object>> documents) {
    List<Map<SolrInputDocument,Map<String,Object>>> docLists = new ArrayList<>();
    Map<SolrInputDocument,Map<String,Object>> docList = null;
    if (this.documents != null) {

      Boolean lastOverwrite = true;
      Integer lastCommitWithin = -1;

      Set<Map.Entry<SolrInputDocument,Map<String,Object>>> entries = this.documents
          .entrySet();
      for (Map.Entry<SolrInputDocument,Map<String,Object>> entry : entries) {
        Map<String,Object> map = entry.getValue();
        Boolean overwrite = null;
        Integer commitWithin = null;
        if (map != null) {
          overwrite = (Boolean) entry.getValue().get(OVERWRITE);
          commitWithin = (Integer) entry.getValue().get(COMMIT_WITHIN);
        }
        if (overwrite != lastOverwrite || commitWithin != lastCommitWithin
            || docLists.size() == 0) {
          docList = new LinkedHashMap<>();
          docLists.add(docList);
        }
        docList.put(entry.getKey(), entry.getValue());
        lastCommitWithin = commitWithin;
        lastOverwrite = overwrite;
      }
    }

    if (docIterator != null) {
      docList = new LinkedHashMap<>();
      docLists.add(docList);
      while (docIterator.hasNext()) {
        SolrInputDocument doc = docIterator.next();
        if (doc != null) {
          docList.put(doc, null);
        }
      }

    }

    return docLists;
  }

  /**
   * @since solr 1.4
   */
  public CustomUpdateRequest writeXML(Writer writer) throws IOException {
    List<Map<SolrInputDocument,Map<String,Object>>> getDocLists = getDocLists(documents);

    for (Map<SolrInputDocument,Map<String,Object>> docs : getDocLists) {

      if ((docs != null && docs.size() > 0)) {
        Map.Entry<SolrInputDocument,Map<String,Object>> firstDoc = docs.entrySet()
            .iterator().next();
        Map<String,Object> map = firstDoc.getValue();
        Integer cw = null;
        Boolean ow = null;
        if (map != null) {
          cw = (Integer) firstDoc.getValue().get(COMMIT_WITHIN);
          ow = (Boolean) firstDoc.getValue().get(OVERWRITE);
        }
        if (ow == null) ow = true;
        int commitWithin = (cw != null && cw != -1) ? cw : this.commitWithin;
        boolean overwrite = ow;
        if (commitWithin > -1 || overwrite != true) {
          writer.write("<add commitWithin=\"" + commitWithin + "\" "
              + "overwrite=\"" + overwrite + "\">");
        } else {
          writer.write("<add>");
        }

        Set<Map.Entry<SolrInputDocument,Map<String,Object>>> entries = docs
            .entrySet();
        for (Map.Entry<SolrInputDocument,Map<String,Object>> entry : entries) {
          ClientUtils.writeXML(entry.getKey(), writer);
        }

        writer.write("</add>");
      }
    }

    return this;
  }

  // --------------------------------------------------------------------------
  // --------------------------------------------------------------------------

  // --------------------------------------------------------------------------
  //
  // --------------------------------------------------------------------------

  public List<SolrInputDocument> getDocuments() {
    if (documents == null) return null;
    List<SolrInputDocument> docs = new ArrayList<>(documents.size());
    docs.addAll(documents.keySet());
    return docs;
  }

  public Map<SolrInputDocument,Map<String,Object>> getDocumentsMap() {
    return documents;
  }

  public Iterator<SolrInputDocument> getDocIterator() {
    return docIterator;
  }

  public boolean isLastDocInBatch() {
    return isLastDocInBatch;
  }

  public void lastDocInBatch() {
    isLastDocInBatch = true;
  }
}
