package org.infinispan.lucene;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.infinispan.Cache;
import org.infinispan.lucene.directory.DirectoryBuilder;
import org.infinispan.manager.CacheContainer;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

/**
 * Basic stress test: one thread writes, some other read.
 * @author Lukasz Moren
 * @author Sanne Grinovero
 */
@Test(groups = "profiling", testName = "lucene.InfinispanDirectoryStressTest")
@SuppressWarnings("unchecked")
public class InfinispanDirectoryStressTest {

   public static final int THREADS_NUM = 50;
   public static final int TURNS_NUM = 300;

   private static final Log log = LogFactory.getLog(InfinispanDirectoryStressTest.class);

   private AtomicInteger writeCount = new AtomicInteger(0);

   public void testInfinispanDirectory() throws Exception {
      final int OPERATIONS = 100;
      CacheContainer cacheContainer = CacheTestSupport.createTestCacheManager();
      Cache cache = cacheContainer.getCache();
      Directory directory = DirectoryBuilder.newDirectoryInstance(cache, cache, cache, "indexName").create();
      CacheTestSupport.initializeDirectory(directory);
      File document = CacheTestSupport.createDummyDocToIndex("document.lucene", 10000);

      for (int i = 0; i < OPERATIONS; i++) {
         CacheTestSupport.doWriteOperation(directory, document);
         CacheTestSupport.doReadOperation(directory);
      }

      IndexReader ir = IndexReader.open(directory);
      IndexSearcher search = new IndexSearcher(ir);
      Term t = new Term("info", "good");
      Query query = new TermQuery(t);
      TopDocs hits = search.search(query, 1);

      ir.close();

      assert OPERATIONS == hits.totalHits;

      directory.close();
      cacheContainer.stop();
   }

   public void testDirectoryWithMultipleThreads() throws Exception {
      final CountDownLatch latch = new CountDownLatch(1);
      List<InfinispanDirectoryThread> threads = new ArrayList<InfinispanDirectoryThread>();
      Cache cache = CacheTestSupport.createTestCacheManager().getCache();
      Directory directory1 = DirectoryBuilder.newDirectoryInstance(cache, cache, cache, "indexName").create();
      CacheTestSupport.initializeDirectory(directory1);

      // second cache joins after index creation: tests proper configuration
      Cache cache2 = CacheTestSupport.createTestCacheManager().getCache(); // dummy cache, to force replication
      Directory directory2 = DirectoryBuilder.newDirectoryInstance(cache2, cache2, cache2, "indexName").create();
      Thread.sleep(3000);

      // create first writing thread
      InfinispanDirectoryThread tr = new InfinispanDirectoryThread(latch, directory1, true);
      threads.add(tr);
      tr.start();
      // others reading threads
      for (int i = 0; i < THREADS_NUM - 1; i++) {
         InfinispanDirectoryThread thread;
         if (i%2==0) {
            thread = new InfinispanDirectoryThread(latch, directory1, false);
         }
         else {
            thread = new InfinispanDirectoryThread(latch, directory2, false);
         }
         threads.add(thread);
         thread.start();
      }

      latch.countDown();

      for (InfinispanDirectoryThread thread : threads) {
         thread.join();
      }

      for (InfinispanDirectoryThread thread : threads) {
         if (thread.e != null) {
            throw thread.e;
         }
      }

      IndexReader indexReader1 = IndexReader.open(directory1);
      IndexSearcher search = new IndexSearcher(indexReader1);
      Term t = new Term("info", "good");
      Query query = new TermQuery(t);
      int expectedDocs = writeCount.get();
      TopDocs hits = search.search(query, 1);

      indexReader1.close();

      assert expectedDocs == hits.totalHits;

      directory1.close();
      directory2.close();
      cache.getCacheManager().stop();
      cache2.getCacheManager().stop();
   }

   class InfinispanDirectoryThread extends Thread {
      Exception e;
      CountDownLatch latch;
      File document;
      Directory dir;
      boolean isWritingThread = false;

      protected InfinispanDirectoryThread(CountDownLatch latch, Directory dir, boolean isWritingThread)
               throws Exception {
         this.latch = latch;
         this.dir = dir;
         this.isWritingThread = isWritingThread;
         document = CacheTestSupport.createDummyDocToIndex("document.lucene", 10000);
      }

      @Override
      public void run() {
         try {
            latch.await();
            for (int i = 0; i < TURNS_NUM; i++) {

               if (!isWritingThread) {
                  CacheTestSupport.doReadOperation(dir);
               }
               else {
                  writeCount.incrementAndGet();
                  CacheTestSupport.doWriteOperation(dir, document);
               }
            }

         }
         catch (Exception ex) {
            log.error("Error", ex);
            e = ex;
         }
      }
   }

}
