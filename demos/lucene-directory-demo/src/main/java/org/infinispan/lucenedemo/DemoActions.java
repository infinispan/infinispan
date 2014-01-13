package org.infinispan.lucenedemo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
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

   /** The Analyzer used in all methods **/
   private static final Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_33);

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
         IndexSearcher searcher = new IndexSearcher(index, true);
         TopDocs topDocs = searcher.search(query, null, 100);// demo limited to 100 documents!
         ScoreDoc[] scoreDocs = topDocs.scoreDocs;
         List<String> list = new ArrayList<String>();
         for (ScoreDoc sd : scoreDocs) {
            Document doc = searcher.doc(sd.doc);
            list.add(doc.get(MAIN_FIELD));
         }
         return list;
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
      IndexWriter iw = new IndexWriter(index, analyzer, MaxFieldLength.UNLIMITED);
      try {
         Document doc = new Document();
         Field field = new Field(MAIN_FIELD, line, Store.YES, Index.ANALYZED);
         doc.add(field);
         iw.addDocument(doc);
         iw.commit();
      } finally {
         iw.close();
      }
   }

   /**
    * Parses a query using the single field as default
    *
    * @throws ParseException
    */
   public Query parseQuery(String queryLine) throws ParseException {
      QueryParser parser = new QueryParser(Version.LUCENE_33, MAIN_FIELD, analyzer);
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
