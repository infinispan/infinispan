package org.infinispan.persistence;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.dummy.DummyInMemoryStore;
import org.infinispan.persistence.support.DelayStore;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * FlushingAsyncStoreTest.
 *
 * @author Sanne Grinovero
 */
@Test(groups = "functional", testName = "persistence.FlushingAsyncStoreTest", singleThreaded = true)
public class FlushingAsyncStoreTest extends SingleCacheManagerTest {

   public static final String CACHE_NAME = "AsyncStoreInMemory";

   public FlushingAsyncStoreTest() {
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder config = getDefaultStandaloneCacheConfig(false);
      config
         .persistence()
            .addStore(DelayStore.ConfigurationBuilder.class)
               .storeName(this.getClass().getName())
               .async().enable()
         .build();
      EmbeddedCacheManager cacheManager = TestCacheManagerFactory.createCacheManager(config);
      cacheManager.defineConfiguration(CACHE_NAME, config.build());
      return cacheManager;
   }

   public void writeOnStorage() throws ExecutionException, InterruptedException, TimeoutException {
      cache = cacheManager.getCache(CACHE_NAME);
      DelayStore store = TestingUtil.getFirstStore(cache);
      store.delayBeforeModification(1);

      cache.put("key1", "value");

      Future<Void> stopFuture = fork(() -> cache.stop());
      assertFalse(stopFuture.isDone());
      assertEquals(0, (int) store.stats().get("write"));

      store.endDelay();
      stopFuture.get(10, TimeUnit.SECONDS);
      assertEquals(1, (int) store.stats().get("write"));
      assertEquals(1, DummyInMemoryStore.getStoreDataSize(store.getStoreName()));
   }
}
