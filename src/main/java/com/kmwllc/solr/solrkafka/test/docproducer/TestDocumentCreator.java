package com.kmwllc.solr.solrkafka.test.docproducer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.solr.common.SolrDocument;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.UUID;

public class TestDocumentCreator implements Iterable<SolrDocument>, Iterator<SolrDocument> {
  private static final ObjectMapper mapper = new ObjectMapper();
  private static final Base64.Encoder encoder = Base64.getEncoder();
  private static final Random random = new Random();
  private final int numDocs;
  private final int docSize;
  private int currDoc = 0;
  private final List<SolrDocument> seededDocs;

  public TestDocumentCreator(int numDocs, int docSize) {
    this.numDocs = numDocs;
    this.docSize = docSize;
    seededDocs = null;
  }

  public TestDocumentCreator(List<SolrDocument> docs) {
    this.seededDocs = docs;
    this.docSize = -1;
    this.numDocs = docs.size();
  }

  public static void main(String[] args) throws IOException {
    Path outputFile = Path.of("src/test/resources/test-documents.json");
    int numDocs = 256;
    if (args.length == 2) {
      outputFile = Path.of(args[0]);
      numDocs = Integer.parseInt(args[1]);
    }
    List<SolrDocument> docs = createDocs(numDocs);
    Files.write(outputFile, mapper.writeValueAsBytes(docs));
  }

  public static SolrDocument createRandomDoc() {
    return createRandomDoc(10000);
  }

  public static SolrDocument createRandomDoc(int docSize) {
    SolrDocument doc = new SolrDocument();
    doc.put("id", UUID.randomUUID().toString());
    doc.put("text", encoder.encodeToString(random.ints(random.nextInt(docSize)).parallel()
        .collect(StringBuilder::new, StringBuilder::append, StringBuilder::append)
        .toString().getBytes(StandardCharsets.UTF_8)));
    doc.put("doc_number", random.nextInt());
    doc.put("doc_double", random.nextDouble());
    doc.put("some_flag", random.nextBoolean());
    return doc;
  }

  public static List<SolrDocument> createDocs(int numDocs) {
    List<SolrDocument> docs = new ArrayList<>();
    for (int i = 0; i < numDocs; i++) {
      docs.add(createRandomDoc());
    }
    return docs;
  }

  @Override
  public Iterator<SolrDocument> iterator() {
    return seededDocs == null ? this : seededDocs.iterator();
  }

  @Override
  public boolean hasNext() {
    return currDoc < numDocs;
  }

  @Override
  public SolrDocument next() {
    if (seededDocs != null) {
      throw new IllegalStateException("Should be using ArrayList iterator rather than doc creator");
    }
    if (currDoc >= numDocs) {
      throw new IllegalStateException("Iterator drained");
    }
    currDoc++;
    return createRandomDoc(docSize);
  }

  public int size() {
    return numDocs;
  }
}
