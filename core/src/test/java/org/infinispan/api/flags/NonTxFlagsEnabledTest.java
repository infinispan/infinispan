package org.infinispan.api.flags;

import org.infinispan.AdvancedCache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.MagicKey;
import org.infinispan.persistence.UnnecessaryLoadingTest;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;

import static org.infinispan.context.Flag.CACHE_MODE_LOCAL;

/**
 * FlagsEnabledTest for non transactional caches.
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "api.flags.NonTxFlagsEnabledTest")
@CleanupAfterMethod
public class NonTxFlagsEnabledTest extends FlagsEnabledTest {

   public NonTxFlagsEnabledTest() {
      super("non-tx-replication");
   }

   @Override
   public void testReplicateSkipCacheLoaderWithinTxInCoordinator(Method m) throws Exception {
      //non transactional cache
   }

   @Override
   public void testReplicateSkipCacheLoaderWithinTxInNonCoordinator(Method m) throws Exception {
      //non transactional cache
   }

   public void testCacheLocalInNonOwner() {
      addClusterEnabledCacheManager(getConfigurationBuilder());
      waitForClusterToForm(cacheName);
      final AdvancedCache<Object, String> cache1 = advancedCache(0, cacheName);
      final AdvancedCache<Object, String> cache2 = advancedCache(1, cacheName);
      final AdvancedCache<Object, String> cache3 = advancedCache(2, cacheName);
      final Object key = new MagicKey("k-no", cache1);

      cache3.withFlags(CACHE_MODE_LOCAL).put(key, "value");

      assertCacheValue(cache3, key, "value");
      assertCacheValue(cache1, key, null);
      assertCacheValue(cache2, key, null);
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
      builder
            .persistence().addStore(UnnecessaryLoadingTest.CountingStoreConfigurationBuilder.class)
            .persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class)
            .clustering().hash().numSegments(2);
      createClusteredCaches(2, cacheName, builder);
   }

   private ConfigurationBuilder getConfigurationBuilder() {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
      builder
            .persistence().addStore(UnnecessaryLoadingTest.CountingStoreConfigurationBuilder.class)
            .persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class)
            .clustering().hash().numSegments(2);
      return builder;
   }
}
