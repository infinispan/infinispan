package org.infinispan.persistence;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.filter.KeyFilter;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.async.AdvancedAsyncCacheLoader;
import org.infinispan.persistence.dummy.DummyInMemoryStore;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.spi.AdvancedCacheLoader;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.io.Serializable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.testng.AssertJUnit.assertEquals;

@Test(groups = "functional", testName = "persistence.PreloadingWithWriteBehindTest")
public class PreloadingWithWriteBehindTest extends SingleCacheManagerTest {

   /**
    * This key will generate a lot of collisions (hashcode = const). Hopefully this will make
    * the race condition from AdvancedCacheLoader#process to appear more ffrequently
    */
   static class KeyWithCollisions implements Serializable {

      @Override
      public int hashCode() {
         return 1;
      }
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder dccc = getDefaultClusteredCacheConfig(CacheMode.LOCAL);
      DummyInMemoryStoreConfigurationBuilder discb = dccc.persistence()
            .addStore(DummyInMemoryStoreConfigurationBuilder.class);
      dccc.transaction().cacheStopTimeout(50, TimeUnit.SECONDS);
      discb
            .async().enabled(true)
            .preload(true)
            .storeName("PreloadingWithWriteBehindTest");
      return TestCacheManagerFactory.createCacheManager(dccc);
   }

   public void testIfPreloadWorkCorrectly() {
      //given
      cache.put("k1","v1");
      cache.put("k2","v2");
      cache.put("k3","v3");

      //when
      getDummyLoader().clearStats();
      cache.stop();
      cache.start();

      Integer loads = getDummyLoader().stats().get("load");

      //then
      assertEquals(3, cache.size());
      assertEquals((Integer)0, loads);
   }

   @Test(timeOut = 10_000)
   public void testIfCanLoadKeysConcurrently() throws Exception {
      final int numberOfEntriesTested = 1_000;
      final int numberOfConcurrentThreads = 16;

      IntStream
              .range(0, numberOfEntriesTested)
              .forEach(i -> cache.put(new KeyWithCollisions(), "value: " + i));

      AdvancedCacheLoader.CacheLoaderTask<Object, String> task = (marshalledEntry, taskContext) -> {};
      KeyFilter<Object> keyFilter = KeyFilter.ACCEPT_ALL_FILTER;
      ExecutorService executeInManyThreads = Executors.newFixedThreadPool(numberOfConcurrentThreads);


      AdvancedAsyncCacheLoader cacheLoader = (AdvancedAsyncCacheLoader) TestingUtil.getCacheLoader(cache);

      //when
      cacheLoader.process(keyFilter, task, executeInManyThreads, true, true);

      //then
      assertEquals(numberOfEntriesTested, cacheLoader.size());
   }

   private DummyInMemoryStore getDummyLoader() {
      return (DummyInMemoryStore) ((AdvancedAsyncCacheLoader)TestingUtil.getCacheLoader(cache)).undelegate();
   }
}
