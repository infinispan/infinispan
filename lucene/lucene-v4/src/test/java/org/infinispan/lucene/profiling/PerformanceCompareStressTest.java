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
package org.infinispan.lucene.profiling;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.infinispan.Cache;
import org.infinispan.lucene.CacheTestSupport;
import org.infinispan.lucene.DirectoryIntegrityCheck;
import org.infinispan.lucene.directory.DirectoryBuilder;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * PerformanceCompareStressTest is useful to get an idea on relative performance between Infinispan
 * in local or clustered mode against a RAMDirectory or FSDirectory. To be reliable set a long
 * DURATION_MS and a number of threads similar to the use case you're interested in: results might
 * vary on the number of threads because of the lock differences. This is not meant as a benchmark
 * but used to detect regressions.
 * 
 * This requires Lucene > 2.9.1 or Lucene > 3.0.0 because of
 * https://issues.apache.org/jira/browse/LUCENE-2095
 * 
 * @author Sanne Grinovero
 * @since 4.0
 */
@SuppressWarnings("unchecked")
@Test(groups = "profiling", testName = "lucene.profiling.PerformanceCompareStressTest", sequential = true)
public class PerformanceCompareStressTest {

   /**
    * The number of terms in the dictionary used as source of terms by the IndexWriter to produce
    * new documents
    */
   private static final int DICTIONARY_SIZE = 800 * 1000;

   /** Concurrent Threads in tests */
   private static final int READER_THREADS = 5;
   private static final int WRITER_THREADS = 1;
   
   private static final int CHUNK_SIZE = 512 * 1024;

   private static final String indexName = "tempIndexName";

   private static final long DURATION_MS = 2 * 60 * 1000;

   private Cache cache;

   private EmbeddedCacheManager cacheFactory;

   @Test
   public void profileTestRAMDirectory() throws InterruptedException, IOException {
      RAMDirectory dir = new RAMDirectory();
      stressTestDirectory(dir, "RAMDirectory");
   }

   @Test
   public void profileTestFSDirectory() throws InterruptedException, IOException {
      File indexDir = new File(new File("."), indexName);
      boolean directoriesCreated = indexDir.mkdirs();
      assert directoriesCreated : "couldn't create directory for FSDirectory test";
      FSDirectory dir = FSDirectory.open(indexDir);
      stressTestDirectory(dir, "FSDirectory");
   }
   
   @Test
   public void profileTestInfinispanDirectoryWithNetworkDelayZero() throws InterruptedException, IOException {
      // TestingUtil.setDelayForCache(cache, 0, 0);
      Directory dir = DirectoryBuilder.newDirectoryInstance(cache, cache, cache, indexName).chunkSize(CHUNK_SIZE).create();
      stressTestDirectory(dir, "InfinispanClustered-delayedIO:0");
      verifyDirectoryState();
   }

   @Test
   public void profileTestInfinispanDirectoryWithNetworkDelay4() throws Exception {
      TestingUtil.setDelayForCache(cache, 4, 4);
      Directory dir = DirectoryBuilder.newDirectoryInstance(cache, cache, cache, indexName).chunkSize(CHUNK_SIZE).create();
      stressTestDirectory(dir, "InfinispanClustered-delayedIO:4");
      verifyDirectoryState();
   }

   @Test
   public void profileTestInfinispanDirectoryWithHighNetworkDelay40() throws Exception {
      TestingUtil.setDelayForCache(cache, 40, 40);
      Directory dir = DirectoryBuilder.newDirectoryInstance(cache, cache, cache, indexName).chunkSize(CHUNK_SIZE).create();
      stressTestDirectory(dir, "InfinispanClustered-delayedIO:40");
      verifyDirectoryState();
   }

   @Test
   public void profileInfinispanLocalDirectory() throws InterruptedException, IOException {
      CacheContainer cacheContainer = CacheTestSupport.createLocalCacheManager();
      try {
         cache = cacheContainer.getCache();
         Directory dir = DirectoryBuilder.newDirectoryInstance(cache, cache, cache, indexName).chunkSize(CHUNK_SIZE).create();
         stressTestDirectory(dir, "InfinispanLocal");
         verifyDirectoryState();
      } finally {
         cacheContainer.stop();
      }
   }

   @Test(enabled=false)//to prevent invocations from some versions of TestNG
   public static void stressTestDirectory(Directory dir, String testLabel) throws InterruptedException, IOException {
      SharedState state = new SharedState(DICTIONARY_SIZE);
      CacheTestSupport.initializeDirectory(dir);
      ExecutorService e = Executors.newFixedThreadPool(READER_THREADS + WRITER_THREADS);
      for (int i = 0; i < READER_THREADS; i++) {
         e.execute(new LuceneReaderThread(dir, state));
      }
      for (int i = 0; i < WRITER_THREADS; i++) {
         e.execute(new LuceneWriterThread(dir, state));
      }
      e.shutdown();
      state.startWaitingThreads();
      Thread.sleep(DURATION_MS);
      long searchesCount = state.incrementIndexSearchesCount(0);
      long writerTaskCount = state.incrementIndexWriterTaskCount(0);
      state.quit();
      boolean terminatedCorrectly = e.awaitTermination(20, TimeUnit.SECONDS);
      AssertJUnit.assertTrue(terminatedCorrectly);
      System.out.println("Test " + testLabel + " run in " + DURATION_MS + "ms:\n\tSearches: " + searchesCount + "\n\t" + "Writes: "
               + writerTaskCount);
   }

   @BeforeMethod
   public void beforeTest() {
      cacheFactory = TestCacheManagerFactory.createClusteredCacheManager(
            CacheTestSupport.createTestConfiguration(TransactionMode.NON_TRANSACTIONAL));
      cacheFactory.start();
      cache = cacheFactory.getCache();
      cache.clear();
   }

   @AfterMethod
   public void afterTest() {
      TestingUtil.killCaches(cache);
      TestingUtil.killCacheManagers(cacheFactory);
      TestingUtil.recursiveFileRemove(indexName);
   }
   
   private void verifyDirectoryState() {
      DirectoryIntegrityCheck.verifyDirectoryStructure(cache, indexName, true);
   }

   /**
    * It's much better to compare performance out of the scope of TestNG by
    * running this directly as TestNG enables assertions.
    * 
    * Suggested test switches:
    * -Xmx2G -Xms2G -XX:MaxPermSize=128M -XX:+HeapDumpOnOutOfMemoryError -Xss512k -XX:HeapDumpPath=/tmp/java_heap -Djava.net.preferIPv4Stack=true -Djgroups.bind_addr=127.0.0.1 -Xbatch -server -XX:+UseCompressedOops -XX:+UseLargePages -XX:LargePageSizeInBytes=2m -XX:+AlwaysPreTouch
    */
   public static void main(String[] args) throws InterruptedException, IOException {
      PerformanceCompareStressTest test = new PerformanceCompareStressTest();
      test.beforeTest();
      try {
         test.profileTestRAMDirectory();
      }
      finally {
         test.afterTest();
      }
      test.beforeTest();
      try {
         test.profileTestFSDirectory();
      }
      finally {
         test.afterTest();
      }
      test.beforeTest();
      try {
         test.profileInfinispanLocalDirectory();
      }
      finally {
         test.afterTest();
      }
      test.beforeTest();
      try {
         test.profileTestInfinispanDirectoryWithNetworkDelayZero();
      }
      finally {
         test.afterTest();
      }
   }

}
