package org.infinispan.statetransfer;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.BaseCustomAsyncInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.test.Exceptions;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.Test;

/**
 * Test if state transfer happens properly on a non-tx invalidation cache.
 *
 * @since 7.0
 */
@Test(groups = "functional", testName = "statetransfer.NonTxStateTransferInvalidationTest")
@CleanupAfterMethod
public class NonTxStateTransferInvalidationTest extends MultipleCacheManagersTest {

   public static final int NUM_KEYS = 10;
   private ConfigurationBuilder dccc;

   @Override
   protected void createCacheManagers() throws Throwable {
      dccc = getDefaultClusteredCacheConfig(CacheMode.INVALIDATION_SYNC, false, true);
      createCluster(dccc, 2);
      waitForClusterToForm();
   }

   public void testStateTransferDisabled() throws Exception {
      // Insert initial data in the cache
      Set<Object> keys = new HashSet<Object>();
      for (int i = 0; i < NUM_KEYS; i++) {
         Object key = "key" + i;
         keys.add(key);
         cache(0).put(key, key);
      }

      log.trace("State transfer happens here");
      // add a third node
      addClusterEnabledCacheManager(dccc);
      waitForClusterToForm();

      log.trace("Checking the values from caches...");
      for (Object key : keys) {
         log.tracef("Checking key: %s", key);
         // check them directly in data container
         InternalCacheEntry d0 = advancedCache(0).getDataContainer().get(key);
         InternalCacheEntry d1 = advancedCache(1).getDataContainer().get(key);
         InternalCacheEntry d2 = advancedCache(2).getDataContainer().get(key);
         assertEquals(key, d0.getValue());
         assertNull(d1);
         assertNull(d2);
      }
   }

   public void testConfigValidation() {
      ConfigurationBuilder builder1 = new ConfigurationBuilder();
      builder1.clustering().cacheMode(CacheMode.INVALIDATION_ASYNC).stateTransfer();
      builder1.validate();

      ConfigurationBuilder builder2 = new ConfigurationBuilder();
      builder2.clustering().cacheMode(CacheMode.INVALIDATION_ASYNC).stateTransfer().fetchInMemoryState(true);
      Exceptions.expectException(CacheConfigurationException.class, builder2::validate);

      ConfigurationBuilder builder3 = new ConfigurationBuilder();
      builder3.clustering().cacheMode(CacheMode.INVALIDATION_ASYNC).persistence()
            .addStore(DummyInMemoryStoreConfigurationBuilder.class);
      builder3.validate();

      ConfigurationBuilder builder4 = new ConfigurationBuilder();
      builder4.clustering().cacheMode(CacheMode.INVALIDATION_ASYNC).persistence()
            .addStore(DummyInMemoryStoreConfigurationBuilder.class).fetchPersistentState(true);
      Exceptions.expectException(CacheConfigurationException.class, builder4::validate);
   }

   public void testInvalidationDuringStateTransfer() throws Exception {
      EmbeddedCacheManager node1 = manager(0);
      Cache<String, Object> node1Cache = node1.getCache();
      EmbeddedCacheManager node2 = manager(1);
      Cache<String, Object> node2Cache = node2.getCache();
      CountDownLatch latch = new CountDownLatch(1);
      node2Cache.getAdvancedCache().getAsyncInterceptorChain().addInterceptor(new BaseCustomAsyncInterceptor() {
         @Override
         public Object visitInvalidateCommand(InvocationContext ctx, InvalidateCommand command) throws
               Throwable {
            latch.await(10, TimeUnit.SECONDS);
            return super.visitInvalidateCommand(ctx, command);
         }
      }, 0);

      String key = "key";
      Future<?> future = fork(() -> {
         node1Cache.putForExternalRead(key, new Object());
         node1Cache.remove(key);
      });

      EmbeddedCacheManager node3 = addClusterEnabledCacheManager(dccc);
      Cache<Object, Object> node3Cache = node3.getCache();
      TestingUtil.waitForStableTopology(caches());
      log.info("Node 3 started");
      latch.countDown();

      future.get(30, TimeUnit.SECONDS);
      assertNull(node1Cache.get(key));
      assertNull(node2Cache.get(key));
      assertNull(node3Cache.get(key));
   }

}
