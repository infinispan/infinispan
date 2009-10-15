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

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.List;
import java.util.ArrayList;

import org.apache.lucene.store.Directory;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Hits;
import org.infinispan.Cache;
import org.infinispan.manager.CacheManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

/**
 * @author Lukasz Moren
 * @author Sanne Grinovero
 */
@SuppressWarnings("deprecation")
@Test(groups = "profiling", testName = "lucene.InfinispanDirectoryTest")
public class InfinispanDirectoryStressTest {

   private static final Log log = LogFactory.getLog(InfinispanDirectoryStressTest.class);

   public static final int THREADS_NUM = 50;
   public static final int TURNS_NUM = 300;

   private AtomicInteger writeCount = new AtomicInteger(0);

   public void testInfinispanDirectory() throws Exception {
      final int OPERATIONS = 100;
      CacheManager cacheManager = CacheTestSupport.createTestCacheManager();
      Cache<CacheKey, Object> cache = cacheManager.getCache();
      Directory directory = new InfinispanDirectory(cache, "indexName");
      File document = CacheTestSupport.createDummyDocToIndex("document.lucene", 10000);

      for (int i = 0; i < OPERATIONS; i++) {
         CacheTestSupport.doWriteOperation(directory, document);
         CacheTestSupport.doReadOperation(directory);
      }

      IndexSearcher search = new IndexSearcher(directory);
      Term t = new Term("info", "good");
      Query query = new TermQuery(t);
      Hits hits = search.search(query);

      assert OPERATIONS == hits.length();

      directory.close();
      cacheManager.stop();
   }

   public void testDirectoryWithMultipleThreads() throws Exception {
      final CountDownLatch latch = new CountDownLatch(1);
      List<InfinispanDirectoryThread> threads = new ArrayList<InfinispanDirectoryThread>();
      Cache<CacheKey, Object> cache = CacheTestSupport.createTestCacheManager().getCache();
      Cache<CacheKey, Object> cache2 = CacheTestSupport.createTestCacheManager().getCache(); // dummy cache, to force replication
      Directory directory = new InfinispanDirectory(cache, "indexName");

      IndexWriter.MaxFieldLength fieldLength = new IndexWriter.MaxFieldLength(IndexWriter.DEFAULT_MAX_FIELD_LENGTH);
      IndexWriter iw = new IndexWriter(directory, new StandardAnalyzer(), true, fieldLength);
      iw.close();

      // create first writing thread
      InfinispanDirectoryThread tr = new InfinispanDirectoryThread(latch, directory, true);
      threads.add(tr);
      tr.start();
      // others reading threads
      for (int i = 0; i < THREADS_NUM - 1; i++) {
         InfinispanDirectoryThread thread = new InfinispanDirectoryThread(latch, directory, false);
         threads.add(thread);
         thread.start();
      }

      latch.countDown();

      for (InfinispanDirectoryThread thread : threads) {
         thread.join();
      }

      for (InfinispanDirectoryThread thread : threads) {
         if (thread.e != null) {
            log.error("Exception was catched during the test: ", thread.e);
            assert false : "Exception during test in parallel thread";
         }
      }

      IndexSearcher search = new IndexSearcher(directory);
      Term t = new Term("info", "good");
      Query query = new TermQuery(t);
      Hits hits = search.search(query);

      assert writeCount.get() == hits.length();

      search.close();
      directory.close();
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
               } else {
                  writeCount.incrementAndGet();
                  CacheTestSupport.doWriteOperation(dir, document);
               }
            }

         } catch (Exception ex) {
            log.error("Error", ex);
            e = ex;
         }
      }
   }

}
