package org.infinispan.iteration.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.iteration.BaseSetupEntryRetrieverTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.fail;

@Test(groups = "functional", testName = "iteration.impl.LocalEntryRetrieverCloseIteratorEarlyTest")
public class LocalEntryRetrieverCloseIteratorEarlyTest extends BaseSetupEntryRetrieverTest {

   public LocalEntryRetrieverCloseIteratorEarlyTest() {
      super(false, CacheMode.LOCAL);
   }
   
   public void testAddEntriesUnblocksWhenIteratorClosed() throws InterruptedException, ExecutionException, TimeoutException {
      Cache cache = cache(0, CACHE_NAME);
      // This cast is okay because we know we are using a local cache
      LocalEntryRetriever retriever = (LocalEntryRetriever) TestingUtil.extractComponent(cache, EntryRetriever.class);
      final int chunkSize = 5;
      final LocalEntryRetriever.Itr iterator = retriever.new Itr(chunkSize);
      
      Future<Void> future = fork(new Callable<Void>() {
         @Override
         public Void call() throws InterruptedException {
            Collection<CacheEntry<Integer, Integer>> entries = new ArrayList<>();
            // We want the addEntries to block because it is 1 larger than the chunk size
            for (int i = 0; i < chunkSize + 1; i++) {
               entries.add(new ImmortalCacheEntry(i, i));
            }
            iterator.addEntries(entries);
            return null;
         }
      });
      // Ensure it is blocked
      try {
         future.get(100, TimeUnit.MILLISECONDS);
         fail("We should have not finished");
      } catch (TimeoutException e) {
         // We should have gone in here
      }
      
      // Close the iterator which should unblock the add entries
      iterator.close();
      
      future.get(10, TimeUnit.SECONDS);
   }
}
