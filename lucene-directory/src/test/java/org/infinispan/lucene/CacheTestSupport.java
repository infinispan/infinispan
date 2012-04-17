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
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockObtainFailedException;
import org.infinispan.config.Configuration;
import org.infinispan.config.Configuration.CacheMode;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.lucene.testutils.LuceneSettings;
import org.infinispan.manager.CacheContainer;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

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
      return TestCacheManagerFactory.createClusteredCacheManager( createTestConfiguration() );
   }

   public static Configuration createTestConfiguration() {
      Configuration c = new Configuration();
      c.setCacheMode(Configuration.CacheMode.DIST_SYNC);
      c.setSyncReplTimeout(10000);
      c.setLockAcquisitionTimeout(10000);
      c.setSyncCommitPhase(true);
      c.setL1CacheEnabled(true);
      c.setExposeJmxStatistics(false);
      c.setSyncRollbackPhase(true);
      c.setEnableDeadlockDetection(false);
      c.setInvocationBatchingEnabled(true);
      return c;
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
      for (int i = 0; i < sz; i++) {
         fw.write(Integer.toHexString(r.nextInt(16)));
      }
      fw.close();
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
         // this is a read
         search = new IndexSearcher(d, true);
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

   public static CacheContainer createLocalCacheManager() {
      GlobalConfiguration globalConfiguration = GlobalConfiguration.getNonClusteredDefault();
      Configuration cfg = new Configuration();
      cfg.setCacheMode(CacheMode.LOCAL);
      cfg.setEnableDeadlockDetection(false);
      cfg.setExposeJmxStatistics(false);
      cfg.setL1CacheEnabled(false);
      cfg.setWriteSkewCheck(false);
      cfg.setInvocationBatchingEnabled(true);
      return TestCacheManagerFactory.createCacheManager(globalConfiguration, cfg);
   }
   
   public static void initializeDirectory(Directory directory) throws IOException {
      IndexWriter iwriter = new IndexWriter(directory, LuceneSettings.analyzer, true, MaxFieldLength.UNLIMITED);
      iwriter.setUseCompoundFile(false);
      iwriter.commit();
      iwriter.close();
      //reopen to check for index
      IndexSearcher searcher = new IndexSearcher(directory, true);
      searcher.close();
   }
   
   /**
    * Used in test to remove all documents containing some term
    * 
    * @param dir The Directory containing the Index to verify
    * @param string
    */
   public static void removeByTerm(Directory dir, String term) throws IOException {
      IndexWriter iw = new IndexWriter(dir, LuceneSettings.analyzer, IndexWriter.MaxFieldLength.UNLIMITED);
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
      IndexSearcher searcher = new IndexSearcher(dir,true);
      Query query = new TermQuery(new Term("body", term));
      TopDocs docs = searcher.search(query, null, expectedResults + 1);
      assert docs.totalHits == expectedResults;
      for (ScoreDoc scoreDoc : docs.scoreDocs) {
         int docId = scoreDoc.doc;
         Document document = searcher.doc(docId);
         String idString = document.get("id");
         assert idString != null;
         Integer idFoundElement = Integer.valueOf(idString);
         assert expectedDocumendIds.contains(idFoundElement);
      }
      searcher.close();
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
      IndexWriter iw = new IndexWriter(dir, LuceneSettings.analyzer, IndexWriter.MaxFieldLength.UNLIMITED);
      Document doc = new Document();
      doc.add(new Field("id", String.valueOf(id), Field.Store.YES, Field.Index.NOT_ANALYZED));
      doc.add(new Field("body", text, Field.Store.NO, Field.Index.ANALYZED));
      iw.addDocument(doc);
      iw.commit();
      iw.close();
   }
   
   public static void optimizeIndex(Directory dir) throws IOException {
      IndexWriter iw = new IndexWriter(dir, LuceneSettings.analyzer, IndexWriter.MaxFieldLength.UNLIMITED);
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
   
}
