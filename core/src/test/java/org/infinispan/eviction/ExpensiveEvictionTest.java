package org.infinispan.eviction;

import java.util.concurrent.TimeUnit;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * @author Sanne Grinovero <sanne@infinispan.org> (C) 2011 Red Hat Inc.
 */
@Test(groups = "profiling", enabled = false, testName = "eviction.ExpensiveEvictionTest")
public class ExpensiveEvictionTest extends SingleCacheManagerTest {

   private final Integer MAX_CACHE_ELEMENTS = 10 * 1000 * 1000;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = new ConfigurationBuilder();
      cfg
         .eviction().strategy(EvictionStrategy.LRU).maxEntries(MAX_CACHE_ELEMENTS)
         .expiration().wakeUpInterval(3000L)
         .build();
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(cfg);
      cache = cm.getCache();
      return cm;
   }

   public void testSimpleEvictionMaxEntries() throws Exception {
      System.out.println("Max entries: " + MAX_CACHE_ELEMENTS);
      for (int i = 0; i < MAX_CACHE_ELEMENTS; i++) {
         Integer integer = Integer.valueOf(i);
         cache.put(integer, integer, 6, TimeUnit.HOURS);
         if (i % 50000 == 0) {
            System.out.println("Elemenents in cache: " + cache.size());
         }
      }
      System.out.println("Finished filling in cache. Now idle while eviting thread works....");
      Thread.sleep(TimeUnit.MILLISECONDS.convert(2, TimeUnit.HOURS));
   }

}
