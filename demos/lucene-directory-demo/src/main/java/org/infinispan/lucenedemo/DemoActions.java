package org.infinispan.lucenedemo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Version;
import org.infinispan.Cache;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.commons.util.InfinispanCollections;

/**
 * DemoActions does some basic operations on the Lucene index,
 * to be used by DemoDriver to show base operations on Lucene.
 *
 * @author Sanne Grinovero
 * @since 4.0
 */
public class DemoActions {

   /** The MAIN_FIELD */
   private static final String MAIN_FIELD = "myField";

   private static final Version luceneVersion = Version.LUCENE_4_10_1;

   /** The Analyzer used in all methods **/
   private static final Analyzer analyzer = new StandardAnalyzer();

   private final Directory index;

   private final Cache<?, ?> cache;

   public DemoActions(Directory index, Cache<?, ?> cache) {
      this.index = index;
      this.cache = cache;
   }

   /**
    * Runs a Query and returns the stored field for each matching document
    * @throws IOException
    */
   public List<String> listStoredValuesMatchingQuery(Query query) {
      try {
         final DirectoryReader reader = DirectoryReader.open(index);
         try {
            IndexSearcher searcher = new IndexSearcher(reader);
            TopDocs topDocs = searcher.search(query, null, 100);// demo limited to 100 documents!
            ScoreDoc[] scoreDocs = topDocs.scoreDocs;
            List<String> list = new ArrayList<String>();
            for (ScoreDoc sd : scoreDocs) {
               Document doc = searcher.doc(sd.doc);
               list.add(doc.get(MAIN_FIELD));
            }
            return list;
         }
         finally {
            reader.close();
         }
      } catch (IOException ioe) {
         // not recommended: in the simple demo this likely means that the index was not yet
         // initialized, so returning empty list.
         return InfinispanCollections.emptyList();
      }
   }

   /**
    * Returns a list of the values of all stored fields
    * @throws IOException
    */
   public List<String> listAllDocuments() {
      MatchAllDocsQuery q = new MatchAllDocsQuery();
      return listStoredValuesMatchingQuery(q);
   }

   /**
    * Creates a new document having just one field containing a string
    *
    * @param line The text snippet to add
    * @throws IOException
    */
   public void addNewDocument(String line) throws IOException {
      IndexWriterConfig config = makeIndexWriterConfig();
      IndexWriter iw = new IndexWriter(index, config);
      try {
         Document doc = new Document();
         TextField field = new TextField(MAIN_FIELD, line, Store.YES);
         doc.add(field);
         iw.addDocument(doc);
         iw.commit();
      } finally {
         iw.close();
      }
   }

   private IndexWriterConfig makeIndexWriterConfig() {
      return new IndexWriterConfig(luceneVersion, analyzer);
   }

   /**
    * Parses a query using the single field as default
    *
    * @throws ParseException
    */
   public Query parseQuery(String queryLine) throws ParseException {
      QueryParser parser = new QueryParser(MAIN_FIELD, analyzer);
      return parser.parse(queryLine);
   }

   /**
    * Returns a list of Addresses of all members in the cluster
    */
   public List<Address> listAllMembers() {
      EmbeddedCacheManager cacheManager = cache.getCacheManager();
      return cacheManager.getMembers();
   }

}
