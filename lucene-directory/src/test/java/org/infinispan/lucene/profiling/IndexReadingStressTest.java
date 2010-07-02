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
package org.infinispan.lucene.profiling;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.RAMDirectory;
import org.infinispan.Cache;
import org.infinispan.lucene.CacheKey;
import org.infinispan.lucene.CacheTestSupport;
import org.infinispan.lucene.InfinispanDirectory;
import org.infinispan.lucene.testutils.ClusteredCacheFactory;
import org.infinispan.lucene.testutils.LuceneSettings;
import org.infinispan.manager.CacheContainer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * This is a stress test meant to compare relative performance of RAMDirectory, FSDirectory,
 * Infinispan local Directory, clustered. Focuses on Search performance; an index is built before
 * the performance measurement is started and is not changed during the searches. To use it set a
 * DURATION_MS as long as you can afford; choose thread number and terms number according to your
 * use case as they will affect the results.
 * 
 * @author Sanne Grinovero
 * @since 4.0
 */
@Test(groups = "profiling", testName = "lucene.profiling.IndexReadingStressTest")
public class IndexReadingStressTest {

   /** Concurrent IndexSearchers used during tests */
   private static final int THREADS = 20;

   /** Test duration **/
   private static final long DURATION_MS = 350000;

   /** Number of Terms written in the index **/
   private static final int TERMS_NUMBER = 200000;

   private static final ClusteredCacheFactory cacheFactory = new ClusteredCacheFactory(CacheTestSupport.createTestConfiguration());

   @Test
   public void profileTestRAMDirectory() throws InterruptedException, IOException {
      RAMDirectory dir = new RAMDirectory();
      testDirectory(dir, "RAMDirectory");
   }

   @Test
   public void profileTestFSDirectory() throws InterruptedException, IOException {
      File indexDir = new File(new File("."), "tempindex");
      indexDir.mkdirs();
      FSDirectory dir = FSDirectory.open(indexDir);
      testDirectory(dir, "FSDirectory");
   }

   @Test
   public void profileTestInfinispanDirectory() throws InterruptedException, IOException {
      // theses default are not for performance settings but meant for problem detection:
      Cache<CacheKey, Object> cache = cacheFactory.createClusteredCache();
      InfinispanDirectory dir = new InfinispanDirectory(cache, "iname");
      testDirectory(dir, "InfinispanClustered");
   }

   @Test
   public void profileInfinispanLocalDirectory() throws InterruptedException, IOException {
      CacheContainer cacheManager = CacheTestSupport.createLocalCacheManager();
      try {
         Cache<CacheKey, Object> cache = cacheManager.getCache();
         InfinispanDirectory dir = new InfinispanDirectory(cache, "iname");
         testDirectory(dir, "InfinispanLocal");
      } finally {
         cacheManager.stop();
      }
   }

   private void testDirectory(Directory dir, String testLabel) throws InterruptedException, IOException {
      SharedState state = fillDirectory(dir);
      ExecutorService e = Executors.newFixedThreadPool(THREADS);
      for (int i = 0; i < THREADS; i++) {
         e.execute(new LuceneReaderThread(dir, state));
      }
      e.shutdown();
      state.startWaitingThreads();
      Thread.sleep(DURATION_MS);
      long searchesCount = state.incrementIndexSearchesCount(0);
      long writerTaskCount = state.incrementIndexWriterTaskCount(0);
      state.quit();
      e.awaitTermination(10, TimeUnit.SECONDS);
      System.out.println("Test " + testLabel + " run in " + DURATION_MS + "ms:\n\tSearches: " + searchesCount + "\n\t" + "Writes: "
               + writerTaskCount);
   }

   private SharedState fillDirectory(Directory directory) throws CorruptIndexException, LockObtainFailedException, IOException {
      CacheTestSupport.initializeDirectory(directory);
      SharedState state = new SharedState(0);
      IndexWriter iwriter = LuceneSettings.openWriter(directory, 100000);
      for (int i = 0; i <= TERMS_NUMBER; i++) {
         Document doc = new Document();
         String term = String.valueOf(i);
         state.addStringWrittenToIndex(term);
         doc.add(new Field("main", term, Store.NO, Index.NOT_ANALYZED));
         iwriter.addDocument(doc);
      }
      iwriter.commit();
      iwriter.close();
      return state;
   }

   @BeforeClass
   public static void beforeTest() {
      cacheFactory.start();
   }

   @AfterClass
   public static void afterTest() {
      cacheFactory.stop();
   }

}
