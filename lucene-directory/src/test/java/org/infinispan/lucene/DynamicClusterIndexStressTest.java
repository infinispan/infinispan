/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockObtainFailedException;
import org.infinispan.Cache;
import org.infinispan.lucene.testutils.ClusteredCacheFactory;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.TestingUtil;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 * DynamicClusterIndexStressTest verifies the index state is healthy while the cluster topology changes.
 * While doing rehashing of the cluster in background it makes changes and searches, verifying the
 * search results.
 * </p>
 * Two pools of strings are maintained: one containing strings which are guaranteed to be found in the index,
 * and one containing the strings which are guaranteed to not be found in the index.
 * Threads take from these pools, check the index state towards them, then write/delete them from
 * index, commit and move them to the appropriate pool to check again or have another thread check them.
 * Different threads own different Directory instances - linked by Infinispan - while the pools are BlockingDeques
 * shared by reference.
 * </p>
 * 
 * Run with -Dbind.address=127.0.0.1 -Djava.net.preferIPv4Stack=true
 * 
 * @author Sanne Grinovero
 * @since 4.0
 */
@Test(groups = "profiling", testName = "lucene.DynamicClusterIndexStressTest", enabled = false,
      description = "This is a 'manual' test and should only be enabled and run when attached to a profiler.")
public class DynamicClusterIndexStressTest {

   private static final int TOTAL_NODES_TO_CREATE = 1000;
   private static final int NODE_EXISTING_MILLISECONDS = 60000;
   private static final int TIME_BETWEEN_NODE_CREATIONS = 1000;
   private static final int CONCURRENCY_LIMIT = 10;
   private static final int STRING_POOL_SIZE = 1000;
   private static final Analyzer anyAnalyzer = new SimpleAnalyzer();

   private final BlockingDeque<String> stringsInIndex = new LinkedBlockingDeque<String>();
   private final BlockingDeque<String> stringsOutOfIndex = new LinkedBlockingDeque<String>();
   private final ClusteredCacheFactory cacheFactory = new ClusteredCacheFactory(CacheTestSupport.createTestConfiguration());

   private volatile boolean failed = false;
   private volatile String failureMessage = "";

//   @Test
//   public void periodicallyAddingANode() throws InterruptedException, LockObtainFailedException, IOException {
//      for (int i = 0; i < STRING_POOL_SIZE; i++) {
//         stringsOutOfIndex.add(String.valueOf(i));
//      }
//      Cache<CacheKey, Object> cache = cacheFactory.createClusteredCache();
//      try {
//         createIndex(cache);
//         runMoreNodes();
//      } finally {
//         cleanup(cache);
//      }
//   }

   /**
    * Initialize the empty index
    * 
    * @param cache to use to contain the index
    * @throws CorruptIndexException
    * @throws LockObtainFailedException
    * @throws IOException
    */
   private void createIndex(Cache<CacheKey, Object> cache) throws CorruptIndexException, LockObtainFailedException, IOException {
      InfinispanDirectory directory = new InfinispanDirectory(cache, "indexName");
      IndexWriter iwriter = new IndexWriter(directory, anyAnalyzer, true, MaxFieldLength.UNLIMITED);
      iwriter.commit();
      iwriter.close();
      IndexSearcher searcher = new IndexSearcher(directory, true);
      searcher.close();
      System.out.println("Index created by " + buildName(cache));
      // verify it can be reopened:
      InfinispanDirectory directory2 = new InfinispanDirectory(cache, "indexName");
      IndexSearcher searcher2 = new IndexSearcher(directory2, true);
      searcher2.close();
   }

   private void runMoreNodes() throws InterruptedException {
      ExecutorService executor = Executors.newFixedThreadPool(CONCURRENCY_LIMIT);
      for (int i = 0; !failed && (i < TOTAL_NODES_TO_CREATE); i++) {
         executor.execute(new LuceneUserThread());
         Thread.sleep(TIME_BETWEEN_NODE_CREATIONS);
      }
      executor.shutdown();
      executor.awaitTermination(1, TimeUnit.HOURS);
      Assert.assertTrue(!failed, failureMessage);
   }
   
   public void testWithoutRehashing() throws InterruptedException{
      ExecutorService executor = Executors.newFixedThreadPool(CONCURRENCY_LIMIT);
      Cache[] caches = new Cache[CONCURRENCY_LIMIT];
      for (int i=0; i<CONCURRENCY_LIMIT; i++){
         caches[i] = cacheFactory.createClusteredCacheWaitingForNodesView(i+1);
      }
      //TODO run the tests
      for (int i=0; i<CONCURRENCY_LIMIT; i++){
         Thread.sleep(250); // there should be some sleep here
         cleanup(caches[i]);
      }
   }
   
   private static String buildName(Cache cache) {
      EmbeddedCacheManager cacheManager = (EmbeddedCacheManager) cache.getCacheManager();
      return Thread.currentThread().getName() +  "[" + cacheManager.getAddress().toString() + "]";
   }

   private class LuceneUserThread implements Runnable {

      private Directory directory;

      @Override
      public void run() {
         if (failed)
            return;
         String name = "";
         try {
            Cache<CacheKey, Object> cache = cacheFactory.createClusteredCache();
            try {
               name = buildName(cache);
               directory = new InfinispanDirectory(cache, "indexName");
               System.out.println("Created Directory in " + name);
               try {
                  runTest();
               } catch (Exception e) {
                  System.out.println("Error in " + name);
                  e.printStackTrace();
                  failed = true;
                  failureMessage = e.getMessage();
               }
            } finally {
               cleanup(cache);
            }
         } catch (InterruptedException e) {
            failed = true;
            failureMessage = e.getMessage();
         } finally {
            System.out.println("Leaving thread " + name);
         }
      }

      private void runTest() throws CorruptIndexException, IOException {
         long finishTime = System.currentTimeMillis() + NODE_EXISTING_MILLISECONDS;
         while (!failed && System.currentTimeMillis() < finishTime) {
            verifyStringsExistInIndex();
            // verifyStringsNotExistInIndex(); //TODO
            addSomeStrings();
            // deleteSomeStrings(); //TODO
         }
      }

      private void addSomeStrings() throws CorruptIndexException, LockObtainFailedException, IOException {
         Set<String> strings = new HashSet<String>();
         stringsOutOfIndex.drainTo(strings, 5);
         IndexWriter iwriter = new IndexWriter(directory, anyAnalyzer, false, MaxFieldLength.UNLIMITED);
         for (String term : strings) {
            Document doc = new Document();
            doc.add(new Field("main", term, Store.NO, Index.NOT_ANALYZED));
            iwriter.addDocument(doc);
         }
         iwriter.commit();
         stringsInIndex.addAll(strings);
         iwriter.close();
      }

      private void verifyStringsExistInIndex() throws CorruptIndexException, IOException {
         // take ownership of some strings, so that no other thread will change status for them:
         Set<String> strings = new HashSet<String>();
         stringsInIndex.drainTo(strings, 50);
         IndexSearcher searcher = new IndexSearcher(directory, true);
         for (String term : strings) {
            Query query = new TermQuery(new Term("main", term));
            TopDocs docs = searcher.search(query, null, 1);
            if (docs.totalHits != 1) {
               failed = true;
               failureMessage = "couldn't find expected term in index";
            }
         }
         // put the strings back at their place:
         stringsInIndex.addAll(strings);
      }

   }

   @BeforeClass
   public void beforeTest() {
      cacheFactory.start();
   }

   @AfterClass
   public void afterTest() {
      cacheFactory.stop();
   }
   
   private static void cleanup(Cache<CacheKey, Object> cache) {
      try {
         TestingUtil.killCaches(cache);
      } finally {
         TestingUtil.killCacheManagers(cache.getCacheManager());
      }
   }

}
