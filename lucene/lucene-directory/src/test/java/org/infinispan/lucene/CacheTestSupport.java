package org.infinispan.lucene;

import org.apache.lucene.document.DateTools;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockObtainFailedException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.lucene.testutils.LuceneSettings;
import org.infinispan.manager.CacheContainer;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.AssertJUnit;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

/**
 * Contains general utilities used by other tests
 */
public abstract class CacheTestSupport {

   private static final Log log = LogFactory.getLog(CacheTestSupport.class);

   protected static CacheContainer createTestCacheManager() {
      return TestCacheManagerFactory.createClusteredCacheManager(
            createTestConfiguration(TransactionMode.NON_TRANSACTIONAL));
   }

   public static ConfigurationBuilder createTestConfiguration(TransactionMode transactionMode) {
      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      builder
            .clustering()
               .cacheMode(CacheMode.DIST_SYNC)
               .stateTransfer()
                  .fetchInMemoryState(true)
               .l1()
                  .enable()
               .sync()
                  .replTimeout(10000)
            .transaction()
               .transactionMode(transactionMode)
            .locking()
               .lockAcquisitionTimeout(10000)
            .invocationBatching()
               .disable()
            .deadlockDetection()
               .disable()
            .jmxStatistics()
               .disable()
            ;
      return builder;
   }

   public static CacheContainer createLocalCacheManager() {
      ConfigurationBuilder builder = createLocalCacheConfiguration();
      return TestCacheManagerFactory.createCacheManager(builder);
   }

   public static ConfigurationBuilder createLocalCacheConfiguration() {
      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      builder
            .clustering()
               .cacheMode(CacheMode.LOCAL)
            .transaction()
               .transactionMode(TransactionMode.NON_TRANSACTIONAL)
            .locking()
               .lockAcquisitionTimeout(10000)
            .invocationBatching()
               .disable()
            .deadlockDetection()
               .disable()
            .jmxStatistics()
               .disable()
            ;
      return builder;
   }

   protected static File createDummyDocToIndex(String fileName, int sz) throws Exception {
      File dummyDocToIndex = new File(fileName);
      if (dummyDocToIndex.exists()) {
         boolean deleted = dummyDocToIndex.delete();
         assert deleted;
      }
      boolean newFileCreated = dummyDocToIndex.createNewFile();
      assert newFileCreated;
      Random r = new Random();
      FileWriter fw = new FileWriter(dummyDocToIndex);
      try {
         for (int i = 0; i < sz; i++) {
            fw.write(Integer.toHexString(r.nextInt(16)));
         }
      } finally {
         fw.close();
      }
      dummyDocToIndex.deleteOnExit();
      return dummyDocToIndex;
   }

   protected static void doWriteOperation(Directory d, File document) throws Exception {
      // this is a write
      IndexWriter writer = null;
      try {
         writer = LuceneSettings.openWriter(d, 2000);
         log.info("IndexWriter was constructed");

         Document doc = new Document();
         doc.add(new Field("path", document.getPath(), Field.Store.YES, Field.Index.NOT_ANALYZED));
         doc.add(new Field("modified", DateTools.timeToString(document.lastModified(), DateTools.Resolution.MINUTE),
                  Field.Store.YES, Field.Index.NOT_ANALYZED));
         doc.add(new Field("contents", new FileReader(document)));
         doc.add(new Field("info", "good", Field.Store.YES, Field.Index.ANALYZED));

         writer.addDocument(doc);
      } catch (LockObtainFailedException lofe) {
         // can happen
      } finally {
         if (writer != null) {
            writer.close();
            log.info("IndexWriter was closed");
         }
      }
   }

   protected static void doReadOperation(Directory d) throws Exception {
      IndexReader indexReader = null;
      IndexSearcher search = null;
      try {
         indexReader = IndexReader.open(d);
         // this is a read
         search = new IndexSearcher(indexReader);
         // dummy query that probably won't return anything
         Term term = new Term( "path", "good" );
         TermQuery termQuery = new TermQuery(term);
         search.search(termQuery, null, 1);
      } finally {
         if (search != null) {
            indexReader.close();
         }
      }
   }

   public static void initializeDirectory(Directory directory) throws IOException {
      IndexWriterConfig indexWriterConfig = new IndexWriterConfig(LuceneSettings.LUCENE_VERSION, LuceneSettings.analyzer);
      IndexWriter iwriter = new IndexWriter(directory, indexWriterConfig);
      iwriter.commit();
      iwriter.close();
      //reopen to check for index
      IndexReader reader = IndexReader.open(directory);
      reader.close();
   }

   /**
    * Used in test to remove all documents containing some term
    *
    * @param dir The Directory containing the Index to verify
    * @param term
    */
   public static void removeByTerm(Directory dir, String term) throws IOException {
      IndexWriterConfig indexWriterConfig = new IndexWriterConfig(LuceneSettings.LUCENE_VERSION, LuceneSettings.analyzer);
      IndexWriter iw = new IndexWriter(dir, indexWriterConfig);
      iw.deleteDocuments(new Term("body", term));
      iw.commit();
      iw.close();
   }

   /**
    * Used in test to verify an Index
    *
    * @param dir The Directory containing the Index to verify
    * @param term a single Term (after analysis) to be searched for
    * @param validDocumentIds The list of document identifiers which should contain the searched-for term
    * @throws IOException
    */
   public static void assertTextIsFoundInIds(Directory dir, String term, Integer... validDocumentIds) throws IOException {
      int expectedResults = validDocumentIds.length;
      Set<Integer> expectedDocumendIds = new HashSet<Integer>(Arrays.asList(validDocumentIds));
      IndexReader reader = IndexReader.open(dir);
      IndexSearcher searcher = new IndexSearcher(reader);
      Query query = new TermQuery(new Term("body", term));
      TopDocs docs = searcher.search(query, null, expectedResults + 1);
      AssertJUnit.assertEquals(expectedResults, docs.totalHits);
      for (ScoreDoc scoreDoc : docs.scoreDocs) {
         int docId = scoreDoc.doc;
         Document document = searcher.doc(docId);
         String idString = document.get("id");
         AssertJUnit.assertNotNull(idString);
         Integer idFoundElement = Integer.valueOf(idString);
         assert expectedDocumendIds.contains(idFoundElement);
      }
      reader.close();
   }

   /**
    * Used in test to add a new Document to an Index; two fields are created: id and body
    *
    * @param dir The Directory containing the Index to modify
    * @param id a sequential number to identify this document (id field)
    * @param text Some text to add to the body field
    * @throws IOException
    */
   public static void writeTextToIndex(Directory dir, int id, String text) throws IOException {
      IndexWriterConfig indexWriterConfig = new IndexWriterConfig(LuceneSettings.LUCENE_VERSION, LuceneSettings.analyzer);
      IndexWriter iw = new IndexWriter(dir, indexWriterConfig);
      Document doc = new Document();
      doc.add(new StringField("id", String.valueOf(id), Field.Store.YES));
      doc.add(new TextField("body", text, Field.Store.NO));
      iw.addDocument(doc);
      iw.commit();
      iw.close();
   }

   /**
    * Optimizing an index is not recommended nowadays, still it's an interesting
    * byte-shuffling exercise to test.
    */
   public static void optimizeIndex(Directory dir) throws IOException {
      IndexWriterConfig indexWriterConfig = new IndexWriterConfig(LuceneSettings.LUCENE_VERSION, LuceneSettings.analyzer);
      IndexWriter iw = new IndexWriter(dir, indexWriterConfig);
      iw.forceMerge(1, true);
      iw.close();
   }

}
