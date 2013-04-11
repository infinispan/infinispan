/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.lucene;

import org.apache.lucene.document.DateTools;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
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
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.lucene.testutils.LuceneSettings;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.DefaultCacheManager;
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
            createTestConfiguration( TransactionMode.NON_TRANSACTIONAL) );
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
                  .enableOnRehash()
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
      IndexSearcher search = null;
      try {
         IndexReader indexReader = IndexReader.open(d);
         // this is a read
         search = new IndexSearcher(indexReader);
         // dummy query that probably won't return anything
         Term term = new Term( "path", "good" );
         TermQuery termQuery = new TermQuery(term);
         search.search(termQuery, null, 1);
      } finally {
         if (search != null) {
            search.close();
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
      searcher.close();
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
      doc.add(new Field("id", String.valueOf(id), Field.Store.YES, Field.Index.NOT_ANALYZED));
      doc.add(new Field("body", text, Field.Store.NO, Field.Index.ANALYZED));
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
   
   /**
    * Useful tool to debug the Lucene invocations into the directory;
    * it prints a thread dump to standard output of only seven lines
    * from the invoker.
    * 
    * @param initialLine The label to print as first line of the stack
    */
   public static void dumpMicroStack(String initialLine) {
      StackTraceElement[] stackTraceElements = Thread.getAllStackTraces().get(Thread.currentThread());
      StringBuilder sb = new StringBuilder(initialLine);
      for (int i = 3; i < 10; i++) {
         sb.append("\n\t");
         sb.append(stackTraceElements[i]);
      }
      sb.append("\n");
      System.out.println(sb.toString());
   }

   /**
    * We should remove this very soon, but it's currently being needed by the JDBC
    * CacheLoader as there is no new alternative for programmatic configuration.
    */
   @Deprecated
   public static org.infinispan.config.Configuration createLegacyTestConfiguration() {
      org.infinispan.config.Configuration c = new org.infinispan.config.Configuration();
      c.setCacheMode(org.infinispan.config.Configuration.CacheMode.DIST_SYNC);
      c.setSyncReplTimeout(10000);
      c.setLockAcquisitionTimeout(10000);
      c.setSyncCommitPhase(true);
      c.setL1CacheEnabled(true);
      c.setExposeJmxStatistics(false);
      c.setSyncRollbackPhase(true);
      c.setEnableDeadlockDetection(false);
      c.setInvocationBatchingEnabled(false);
      return c;
   }

}
