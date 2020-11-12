package org.infinispan.invalidation;

import static org.testng.AssertJUnit.assertEquals;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.util.ControlledRpcManager;
import org.testng.annotations.Test;

/**
 * Test that non-tx locking does not cause deadlocks in sync invalidation caches.
 *
 * <p>See ISPN-12489</p>
 *
 * @author Dan Berindei
 */
@Test(groups = "functional", testName = "invalidation.NonTxInvalidationLockingTest")
public class NonTxInvalidationLockingTest extends MultipleCacheManagersTest {
   private static final String KEY = "key";
   private static final String VALUE1 = "value1";
   private static final Object VALUE2 = "value2";
   private static final String CACHE = "nontx";

   @Override
   protected void createCacheManagers() throws Throwable {
      addClusterEnabledCacheManager();
      addClusterEnabledCacheManager();

      defineCache(CACHE);
      waitForClusterToForm(CACHE);
   }

   private void defineCache(String cacheName) {
      ConfigurationBuilder config = buildConfig();
      manager(0).defineConfiguration(cacheName, config.build());
      manager(1).defineConfiguration(cacheName, config.build());
   }

   private ConfigurationBuilder buildConfig() {
      ConfigurationBuilder cacheConfig = new ConfigurationBuilder();
      cacheConfig.clustering().cacheMode(CacheMode.INVALIDATION_SYNC)
                 .stateTransfer().fetchInMemoryState(false)
                 .transaction().transactionMode(TransactionMode.NON_TRANSACTIONAL)
                 .persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class)
                 .storeName(NonTxInvalidationLockingTest.class.getName())
                 .build();
      return cacheConfig;
   }

   public void testConcurrentWritesFromDifferentNodes() throws Exception {

      Cache<Object, Object> cache1 = cache(0, CACHE);
      ControlledRpcManager rpc1 = ControlledRpcManager.replaceRpcManager(cache1);
      Cache<Object, Object> cache2 = cache(1, CACHE);
      ControlledRpcManager rpc2 = ControlledRpcManager.replaceRpcManager(cache2);

      CompletableFuture<ControlledRpcManager.BlockedRequest<InvalidateCommand>> invalidate1 =
            rpc1.expectCommandAsync(InvalidateCommand.class);
      CompletableFuture<Object> put1 = cache1.putAsync(KEY, VALUE1);

      CompletableFuture<ControlledRpcManager.BlockedRequest<InvalidateCommand>> invalidate2 =
            rpc2.expectCommandAsync(InvalidateCommand.class);
      CompletableFuture<Object> put2 = cache2.putAsync(KEY, VALUE2);

      ControlledRpcManager.SentRequest sentInvalidate1 = invalidate1.join().send();
      ControlledRpcManager.SentRequest sentInvalidate2 = invalidate2.join().send();

      sentInvalidate1.expectAllResponses().receive();
      sentInvalidate2.expectAllResponses().receive();

      put1.get(10, TimeUnit.SECONDS);
      put2.get(10, TimeUnit.SECONDS);

      assertEquals(VALUE1, cache1.get(KEY));
      assertEquals(VALUE2, cache2.get(KEY));
   }

}
