package com.kmwllc.solr.solrkafka.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.solr.common.SolrDocument;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Random;
import java.util.UUID;

public class TestDocumentCreator {
  private static final ObjectMapper mapper = new ObjectMapper();
  private static final Base64.Encoder encoder = Base64.getEncoder();
  private final int numDocs;

  public TestDocumentCreator(int numDocs) {
    this.numDocs = numDocs;
  }

  public static void main(String[] args) throws IOException {
    Path outputFile = Path.of("src/test/resources/test-documents.json");
    int numDocs = 256;
    if (args.length == 2) {
      outputFile = Path.of(args[0]);
      numDocs = Integer.parseInt(args[1]);
    }
    TestDocumentCreator creator = new TestDocumentCreator(numDocs);
    List<SolrDocument> docs = creator.createDocs();
    Files.write(outputFile, mapper.writeValueAsBytes(docs));
  }

  public List<SolrDocument> createDocs() {
    List<SolrDocument> docs = new ArrayList<>();
    Random random = new Random();
    for (int i = 0; i < numDocs; i++) {
      SolrDocument doc = new SolrDocument();
      doc.put("id", UUID.randomUUID().toString());
      doc.put("text", encoder.encodeToString(random.ints(random.nextInt(10000)).parallel()
          .collect(StringBuilder::new, StringBuilder::append, StringBuilder::append)
          .toString().getBytes(StandardCharsets.UTF_8)));
      doc.put("doc_number", i);
      doc.put("some_flag", random.nextBoolean());
      docs.add(doc);
    }
    return docs;
  }
}
