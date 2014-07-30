package org.infinispan.lucene.testutils;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 *
 * Utilities to read and write Lucene indexes
 *
 * @author gustavonalle
 * @since 7.0
 */
public class LuceneUtils {

   private LuceneUtils() {
   }

   /**
    * Read all terms from a field
    *
    * @param field the field in the document to load terms from
    * @param directory Any directory implementation
    * @return Unique terms represented as UTF-8
    * @throws IOException
    */
   public static Set<String> readTerms(String field, Directory directory) throws IOException {
      try (DirectoryReader reader = DirectoryReader.open(directory)) {
         Set<String> termStrings = new TreeSet<>();
         for (AtomicReaderContext atomicReaderContext : reader.leaves()) {
            AtomicReader atomicReader = atomicReaderContext.reader();
            TermsEnum iterator = atomicReader.terms(field).iterator(null);
            BytesRef next = iterator.next();
            while (next != null) {
               termStrings.add(iterator.term().utf8ToString());
               next = iterator.next();
            }
         }
         return termStrings;
      }
   }

   /**
    * Counts the documents
    * @param directory Directory
    * @return the number of docs,including all segments
    * @throws IOException
    */
   public static int numDocs(Directory directory) throws IOException {
      try (DirectoryReader reader = DirectoryReader.open(directory)) {
         return reader.numDocs();
      }
   }

   /**
    * Collect all documents from an index
    * @param directory Directory
    * @param limit maximum number of documents to collect
    * @return List of Documents
    * @throws IOException
    */
   public static List<Document> collect(Directory directory, int limit) throws IOException {
      try (DirectoryReader reader = DirectoryReader.open(directory)) {
         MatchAllDocsQuery allDocsQuery = new MatchAllDocsQuery();
         List<Document> docs = new ArrayList<>(limit);
         IndexSearcher indexSearcher = new IndexSearcher(reader);
         TopDocs topDocs = indexSearcher.search(allDocsQuery, limit);
         for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
            docs.add(indexSearcher.doc(scoreDoc.doc));
         }
         return docs;
      }
   }


}
