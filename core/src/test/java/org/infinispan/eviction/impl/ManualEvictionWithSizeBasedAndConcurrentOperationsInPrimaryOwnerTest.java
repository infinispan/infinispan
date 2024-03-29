package org.infinispan.eviction.impl;

import static org.infinispan.test.TestingUtil.extractComponent;
import static org.testng.AssertJUnit.assertEquals;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.infinispan.Cache;
import org.infinispan.commands.write.EvictCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.impl.AbstractDelegatingInternalDataContainer;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.interceptors.AsyncInterceptor;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.interceptors.impl.CacheLoaderInterceptor;
import org.infinispan.interceptors.impl.CacheWriterInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.cachelistener.annotation.CacheEntriesEvicted;
import org.infinispan.notifications.cachelistener.event.CacheEntriesEvictedEvent;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests manual eviction with concurrent read and/or write operation. This test has passivation disabled and the
 * eviction happens in the primary owner
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "eviction.ManualEvictionWithSizeBasedAndConcurrentOperationsInPrimaryOwnerTest", singleThreaded = true)
public class ManualEvictionWithSizeBasedAndConcurrentOperationsInPrimaryOwnerTest extends EvictionWithConcurrentOperationsTest {

   protected EmbeddedCacheManager otherCacheManager;

   @AfterMethod(alwaysRun = true)
   public void stopSecondCacheManager() {
      if (otherCacheManager != null) {
         otherCacheManager.getCache().stop();
         otherCacheManager.stop();
         otherCacheManager = null;
      }
   }

   @BeforeMethod(alwaysRun = true)
   public void startSecondCacheManager() throws Exception {
      if (otherCacheManager == null) {
         otherCacheManager = createCacheManager();
      } else {
         AssertJUnit.fail("Other cache manager should not be set!");
      }
      Cache otherCache = otherCacheManager.getCache();
      TestingUtil.waitForNoRebalance(cache, otherCache);
   }

   @Override
   public void testScenario1() throws Exception {
      final Object key1 = createSameHashCodeKey("key1");
      initializeKeyAndCheckData(key1, "v1");

      final AfterPassivationOrCacheWriter controller = new AfterPassivationOrCacheWriter().injectThis(cache);
      final Latch latch = new Latch();
      controller.beforeEvict = () -> latch.blockIfNeeded();

      //this will trigger the eviction of key1. key1 eviction will be blocked in the latch
      latch.enable();
      Future<Void> evict = evictWithFuture(key1);
      latch.waitToBlock(30, TimeUnit.SECONDS);

      //the eviction was trigger and it blocked before passivation
      assertEquals("Wrong value for key " + key1 + " in get operation.", "v1", cache.get(key1));

      //let the eviction continue and wait for put
      latch.disable();
      evict.get(30, TimeUnit.SECONDS);

      assertNotInMemory(key1, "v1");
   }

   @Override
   public void testScenario2() throws Exception {
      final Object key1 = createSameHashCodeKey("key1");
      initializeKeyAndCheckData(key1, "v1");

      final Latch latch = new Latch();
      replaceControlledDataContainer(latch);

      //this will trigger the eviction of key1. key1 eviction will be blocked in the latch
      latch.enable();
      Future<Void> evict = evictWithFuture(key1);
      latch.waitToBlock(30, TimeUnit.SECONDS);

      //the eviction was trigger and it blocked before passivation
      assertEquals("Wrong value for key " + key1 + " in get operation.", "v1", cache.get(key1));

      //let the eviction continue and wait for put
      latch.disable();
      evict.get(30, TimeUnit.SECONDS);

      assertNotInMemory(key1, "v1");
   }

   @Override
   public void testScenario3() throws Exception {
      final Object key1 = createSameHashCodeKey("key1");
      initializeKeyAndCheckData(key1, "v1");

      final Latch latch = new Latch();
      final SyncEvictionListener evictionListener = new SyncEvictionListener() {
         @CacheEntriesEvicted
         @Override
         public void evicted(CacheEntriesEvictedEvent event) {
            if (event.getEntries().containsKey(key1)) {
               latch.blockIfNeeded();
            }
         }
      };
      cache.addListener(evictionListener);

      //this will trigger the eviction of key1. key1 eviction will be blocked in the latch
      latch.enable();
      Future<Void> evict = evictWithFuture(key1);
      latch.waitToBlock(30, TimeUnit.SECONDS);

      if (passivation) {
         Future<Object> getFuture = fork(() -> cache.get(key1));

         // Get will be blocked because eviction notification is not yet complete - which is holding orderer
         // CacheLoader requires acquiring orderer so it can update the data container properly
         TestingUtil.assertNotDone(getFuture);

         //let the eviction continue and wait for get to complete (which will put it back in memory)
         latch.disable();
         evict.get(30, TimeUnit.SECONDS);
         assertEquals("v1", getFuture.get(10, TimeUnit.SECONDS));

         assertInMemory(key1, "v1");
      } else {
         //the eviction was triggered and the key is no longer in the map
         assertEquals("Wrong value for key " + key1 + " in get operation.", "v1", cache.get(key1));

         //let the eviction continue and wait for put
         latch.disable();
         evict.get(30, TimeUnit.SECONDS);

         assertInMemory(key1, "v1");
      }
   }

   @Override
   public void testScenario4() throws Exception {
      final Object key1 = createSameHashCodeKey("key1");
      initializeKeyAndCheckData(key1, "v1");

      final Latch readLatch = new Latch();
      final Latch writeLatch = new Latch();
      final AtomicBoolean firstGet = new AtomicBoolean(false);
      final AfterEntryWrappingInterceptor afterEntryWrappingInterceptor = new AfterEntryWrappingInterceptor()
            .injectThis(cache);
      afterEntryWrappingInterceptor.beforeGet = () -> {
         if (firstGet.compareAndSet(false, true)) {
            readLatch.blockIfNeeded();
         }
      };
      final SyncEvictionListener evictionListener = new SyncEvictionListener() {
               @CacheEntriesEvicted
               @Override
               public void evicted(CacheEntriesEvictedEvent event) {
                  if (event.getEntries().containsKey(key1)) {
                     writeLatch.blockIfNeeded();
                  }
               }
            };
      cache.addListener(evictionListener);

      //this will trigger the eviction of key1. key1 eviction will be blocked in the latch
      readLatch.enable();
      Future<Void> evict = evictWithFuture(key1);
      writeLatch.waitToBlock(30, TimeUnit.SECONDS);

      //the eviction was trigger and the key is no longer in the map
      Future<Object> get = fork(() -> cache.get(key1));
      readLatch.waitToBlock(30, TimeUnit.SECONDS);

      //the first read is blocked. it has check the data container and it didn't found any value
      //this second get should not block anywhere and it should fetch the value from persistence
      assertEquals("Wrong value for key " + key1 + " in get operation.", "v1", cache.get(key1));

      //let the eviction continue and wait for put
      writeLatch.disable();
      evict.get(30, TimeUnit.SECONDS);

      //let the second get continue
      readLatch.disable();
      assertEquals("Wrong value for key " + key1 + " in get operation.", "v1", get.get(30, TimeUnit.SECONDS));

      assertInMemory(key1, "v1");
   }

   @Override
   public void testScenario5() throws Exception {
      final Object key1 = createSameHashCodeKey("key1");
      initializeKeyAndCheckData(key1, "v1");

      final Latch readLatch = new Latch();
      final Latch writeLatch = new Latch();
      final AfterEntryWrappingInterceptor afterEntryWrappingInterceptor = new AfterEntryWrappingInterceptor()
            .injectThis(cache);
      afterEntryWrappingInterceptor.beforeGet = () -> readLatch.blockIfNeeded();
      final SyncEvictionListener evictionListener = new SyncEvictionListener() {
               @CacheEntriesEvicted
               @Override
               public void evicted(CacheEntriesEvictedEvent event) {
                  if (event.getEntries().containsKey(key1)) {
                     writeLatch.blockIfNeeded();
                  }
               }
            };
      cache.addListener(evictionListener);

      //this will trigger the eviction of key1. key1 eviction will be blocked in the latch
      readLatch.enable();
      Future<Void> evict = evictWithFuture(key1);
      writeLatch.waitToBlock(30, TimeUnit.SECONDS);

      //the eviction was trigger and the key is no longer in the map
      Future<Object> get = fork(() -> cache.get(key1));
      readLatch.waitToBlock(30, TimeUnit.SECONDS);

      //let the eviction continue
      writeLatch.disable();

      //the first read is blocked. it has check the data container and it didn't found any value
      //this second get should not block anywhere and it should fetch the value from persistence
      assertEquals("Wrong value for key " + key1 + " in put operation.", "v1", cache.put(key1, "v3"));

      evict.get(30, TimeUnit.SECONDS);

      //let the get continue
      readLatch.disable();
      assertEquals("Wrong value for key " + key1 + " in get operation.", "v3", get.get(30, TimeUnit.SECONDS));

      assertInMemory(key1, "v3");
   }

   @Override
   public void testScenario6() throws Exception {
      final Object key1 = createSameHashCodeKey("key1");
      initializeKeyAndCheckData(key1, "v1");

      final Latch readLatch = new Latch();
      final Latch writeLatch = new Latch();
      final Latch writeLatch2 = new Latch();
      final AfterEntryWrappingInterceptor afterEntryWrappingInterceptor = new AfterEntryWrappingInterceptor()
            .injectThis(cache);
      afterEntryWrappingInterceptor.beforeGet = () -> readLatch.blockIfNeeded();
      afterEntryWrappingInterceptor.afterPut = () -> writeLatch2.blockIfNeeded();
      final SyncEvictionListener evictionListener = new SyncEvictionListener() {
               @CacheEntriesEvicted
               @Override
               public void evicted(CacheEntriesEvictedEvent event) {
                  if (event.getEntries().containsKey(key1)) {
                     writeLatch.blockIfNeeded();
                  }
               }
            };
      cache.addListener(evictionListener);

      //this will trigger the eviction of key1. key1 eviction will be blocked in the latch
      readLatch.enable();
      Future<Void> evict = evictWithFuture(key1);
      writeLatch.waitToBlock(30, TimeUnit.SECONDS);

      //the eviction was trigger and the key is no longer in the map
      Future<Object> get = fork(() -> cache.get(key1));
      readLatch.waitToBlock(30, TimeUnit.SECONDS);

      //let the eviction continue
      writeLatch.disable();

      Future<Object> put2 = fork(() -> cache.put(key1, "v3"));

      evict.get(30, TimeUnit.SECONDS);

      //wait until the 2nd put writes to persistence
      writeLatch2.waitToBlock(30, TimeUnit.SECONDS);

      //let the get continue
      readLatch.disable();
      assertPossibleValues(key1, get.get(30, TimeUnit.SECONDS), "v1", "v3");

      writeLatch2.disable();
      assertEquals("Wrong value for key " + key1 + " in get operation.", "v1", put2.get(30, TimeUnit.SECONDS));

      assertInMemory(key1, "v3");
   }

   @Override
   protected void configurePersistence(ConfigurationBuilder builder) {
      builder.persistence().passivation(false).addStore(DummyInMemoryStoreConfigurationBuilder.class)
            .storeName(storeName + storeNamePrefix.getAndIncrement());
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = getDefaultStandaloneCacheConfig(false);
      builder.clustering().cacheMode(CacheMode.DIST_SYNC)
            .hash().numOwners(2).numSegments(2);
      configurePersistence(builder);
      configureEviction(builder);
      return TestCacheManagerFactory.createClusteredCacheManager(new EvictionWithConcurrentOperationsSCIImpl(), builder);
   }

   protected Object createSameHashCodeKey(String name) {
      final Address address = cache.getAdvancedCache().getRpcManager().getAddress();
      DistributionManager distributionManager = cache.getAdvancedCache().getDistributionManager();
      int hashCode = 0;
      SameHashCodeKey key = new SameHashCodeKey(name, hashCode);
      while (!distributionManager.getCacheTopology().getDistribution(key).primary().equals(address)) {
         hashCode++;
         key = new SameHashCodeKey(name, hashCode);
      }
      return key;
   }

   protected final Future<Void> evictWithFuture(final Object key) {
      return fork(() -> {
         cache.evict(key);
         return null;
      });
   }

   private void replaceControlledDataContainer(final Latch latch) {
      InternalDataContainer current = TestingUtil.extractComponent(cache, InternalDataContainer.class);
      //noinspection unchecked
      InternalDataContainer controlledDataContainer = new AbstractDelegatingInternalDataContainer() {
         @Override
         protected InternalDataContainer delegate() {
            return current;
         }

         @Override
         public void evict(Object key) {
            latch.blockIfNeeded();
            super.evict(key);
         }

         @Override
         public CompletionStage<Void> evict(int segment, Object key) {
            latch.blockIfNeeded();
            return super.evict(segment, key);
         }
      };
      TestingUtil.replaceComponent(cache, InternalDataContainer.class, controlledDataContainer, true);
   }

   class AfterPassivationOrCacheWriter extends ControlledCommandInterceptor {

      volatile Runnable beforeEvict;
      volatile Runnable afterEvict;

      public AfterPassivationOrCacheWriter injectThis(Cache<Object, Object> injectInCache) {
         AsyncInterceptorChain chain = extractComponent(injectInCache, AsyncInterceptorChain.class);
         AsyncInterceptor interceptor = chain.findInterceptorExtending(CacheWriterInterceptor.class);
         if (interceptor == null) {
            interceptor = chain.findInterceptorExtending(CacheLoaderInterceptor.class);
         }
         if (interceptor == null) {
            throw new IllegalStateException("Should not happen!");
         }
         chain.addInterceptorAfter(this, interceptor.getClass());
         return this;
      }

      @Override
      public Object visitEvictCommand(InvocationContext ctx, EvictCommand command) throws Throwable {
         return handle(ctx, command, beforeEvict, afterEvict);
      }
   }
}
