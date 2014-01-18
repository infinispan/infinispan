package org.infinispan.eviction;

import org.infinispan.Cache;
import org.infinispan.commands.write.EvictCommand;
import org.infinispan.commons.util.concurrent.ParallelIterableMap.KeyValueAction;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.interceptors.CacheWriterInterceptor;
import org.infinispan.interceptors.DistCacheWriterInterceptor;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.interceptors.PassivationInterceptor;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.annotation.CacheEntriesEvicted;
import org.infinispan.notifications.cachelistener.event.CacheEntriesEvictedEvent;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.spi.AdvancedCacheLoader.KeyFilter;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.jboss.util.NotImplementedException;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.testng.AssertJUnit.assertEquals;

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
      TestingUtil.waitForRehashToComplete(cache, otherCache);
   }

   @Override
   public void testScenario1() throws Exception {
      final Object key1 = createSameHashCodeKey("key1");
      initializeKeyAndCheckData(key1, "v1");

      final AfterPassivationOrCacheWriter controller = new AfterPassivationOrCacheWriter().injectThis(cache);
      final Latch latch = new Latch();
      controller.beforeEvict = new Runnable() {
         @Override
         public void run() {
            latch.blockIfNeeded();
         }
      };

      //this will trigger the eviction of key1. key1 eviction will be blocked in the latch
      latch.enable();
      Future<Void> evict = evictWithFuture(key1);
      latch.waitToBlock(30, TimeUnit.SECONDS);

      //the eviction was trigger and it blocked before passivation
      assertEquals("Wrong value for key " + key1 + " in get operation.", "v1", cache.get(key1));

      //let the eviction continue and wait for put
      latch.disable();
      evict.get();

      assertNotInMemory(key1, "v1");
   }

   @Override
   public void testScenario2() throws Exception {
      final Object key1 = createSameHashCodeKey("key1");
      initializeKeyAndCheckData(key1, "v1");

      final Latch latch = new Latch();
      final ControlledDataContainer controller = replaceControlledDataContainer(latch);

      //this will trigger the eviction of key1. key1 eviction will be blocked in the latch
      latch.enable();
      Future<Void> evict = evictWithFuture(key1);
      latch.waitToBlock(30, TimeUnit.SECONDS);

      //the eviction was trigger and it blocked before passivation
      assertEquals("Wrong value for key " + key1 + " in get operation.", "v1", cache.get(key1));

      //let the eviction continue and wait for put
      latch.disable();
      evict.get();

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

      //the eviction was trigger and the key is no longer in the map
      assertEquals("Wrong value for key " + key1 + " in get operation.", "v1", cache.get(key1));

      //let the eviction continue and wait for put
      latch.disable();
      evict.get();

      assertInMemory(key1, "v1");
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
      afterEntryWrappingInterceptor.beforeGet = new Runnable() {
         @Override
         public void run() {
            if (firstGet.compareAndSet(false, true)) {
               readLatch.blockIfNeeded();
            }
         }
      };
      final SyncEvictionListener evictionListener = new

            SyncEvictionListener() {
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
      Future<Object> get = cache.getAsync(key1);
      readLatch.waitToBlock(30, TimeUnit.SECONDS);

      //the first read is blocked. it has check the data container and it didn't found any value
      //this second get should not block anywhere and it should fetch the value from persistence
      assertEquals("Wrong value for key " + key1 + " in get operation.", "v1", cache.get(key1));

      //let the eviction continue and wait for put
      writeLatch.disable();
      evict.get();

      //let the second get continue
      readLatch.disable();
      assertEquals("Wrong value for key " + key1 + " in get operation.", "v1", get.get());

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
      afterEntryWrappingInterceptor.beforeGet = new Runnable() {
         @Override
         public void run() {
            readLatch.blockIfNeeded();

         }
      };
      final SyncEvictionListener evictionListener = new

            SyncEvictionListener() {
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
      Future<Object> get = cache.getAsync(key1);
      readLatch.waitToBlock(30, TimeUnit.SECONDS);

      //let the eviction continue
      writeLatch.disable();

      //the first read is blocked. it has check the data container and it didn't found any value
      //this second get should not block anywhere and it should fetch the value from persistence
      assertEquals("Wrong value for key " + key1 + " in put operation.", "v1", cache.put(key1, "v3"));

      evict.get();

      //let the get continue
      readLatch.disable();
      assertEquals("Wrong value for key " + key1 + " in get operation.", "v3", get.get());

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
      afterEntryWrappingInterceptor.beforeGet = new Runnable() {
         @Override
         public void run() {
            readLatch.blockIfNeeded();

         }
      };
      afterEntryWrappingInterceptor.afterPut = new

            Runnable() {
               @Override
               public void run() {
                  writeLatch2.blockIfNeeded();
               }
            };
      final SyncEvictionListener evictionListener = new

            SyncEvictionListener() {
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
      Future<Object> get = cache.getAsync(key1);
      readLatch.waitToBlock(30, TimeUnit.SECONDS);

      //let the eviction continue
      writeLatch.disable();

      Future<Object> put2 = cache.putAsync(key1, "v3");

      evict.get();

      //wait until the 2nd put writes to persistence
      writeLatch2.waitToBlock(30, TimeUnit.SECONDS);

      //let the get continue
      readLatch.disable();
      assertPossibleValues(key1, get.get(), "v1", "v3");

      writeLatch2.disable();
      assertEquals("Wrong value for key " + key1 + " in get operation.", "v1", put2.get());

      assertInMemory(key1, "v3");
   }

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
      return TestCacheManagerFactory.createClusteredCacheManager(builder);
   }

   protected Object createSameHashCodeKey(String name) {
      final Address address = cache.getAdvancedCache().getRpcManager().getAddress();
      DistributionManager distributionManager = cache.getAdvancedCache().getDistributionManager();
      int hashCode = 0;
      SameHashCodeKey key = new SameHashCodeKey(name, hashCode);
      while (!distributionManager.getPrimaryLocation(key).equals(address)) {
         hashCode++;
         key = new SameHashCodeKey(name, hashCode);
      }
      return key;
   }

   protected final Future<Void> evictWithFuture(final Object key) {
      return fork(new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            cache.evict(key);
            return null;
         }
      });
   }

   private ControlledDataContainer replaceControlledDataContainer(final Latch latch) {
      DataContainer current = TestingUtil.extractComponent(cache, DataContainer.class);
      ClusteringDependentLogic clusteringDependentLogic = TestingUtil.extractComponent(cache, ClusteringDependentLogic.class);
      ControlledDataContainer controlledDataContainer = new ControlledDataContainer(current, clusteringDependentLogic);
      controlledDataContainer.beforeRemove = new Runnable() {
         @Override
         public void run() {
            latch.blockIfNeeded();
         }
      };
      TestingUtil.replaceComponent(cache, DataContainer.class, controlledDataContainer, true);
      return controlledDataContainer;
   }

   public static class SameHashCodeKey implements Serializable {

      private final String name;
      private final int hashCode;

      public SameHashCodeKey(String name, int hashCode) {
         this.name = name;
         this.hashCode = hashCode;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         SameHashCodeKey that = (SameHashCodeKey) o;

         return name.equals(that.name);

      }

      @Override
      public int hashCode() {
         return hashCode;
      }

      @Override
      public String toString() {
         return name;
      }
   }

   private class ControlledDataContainer implements DataContainer {

      private final DataContainer delegate;
      private final ClusteringDependentLogic clusteringDependentLogic;
      private volatile Runnable beforeRemove;

      private ControlledDataContainer(DataContainer delegate, ClusteringDependentLogic clusteringDependentLogic) {
         this.delegate = delegate;
         this.clusteringDependentLogic = clusteringDependentLogic;
      }

      @Override
      public InternalCacheEntry get(Object k) {
         return delegate.get(k);
      }

      @Override
      public InternalCacheEntry peek(Object k) {
         return delegate.peek(k);
      }

      @Override
      public void put(Object k, Object v, Metadata metadata) {
         delegate.put(k, v, metadata);
      }

      @Override
      public boolean containsKey(Object k) {
         return delegate.containsKey(k);
      }

      @Override
      public InternalCacheEntry remove(Object k) {
         run(beforeRemove, k);
         return delegate.remove(k);
      }

      @Override
      public int size() {
         return delegate.size();
      }

      @Override
      @Stop(priority = 999)
      public void clear() {
         delegate.clear();
      }

      @Override
      public Set<Object> keySet() {
         return delegate.keySet();
      }

      @Override
      public Collection<Object> values() {
         return delegate.values();
      }

      @Override
      public Set<InternalCacheEntry> entrySet() {
         return delegate.entrySet();
      }

      @Override
      public void purgeExpired() {
         delegate.purgeExpired();
      }

      @Override
      public Iterator<InternalCacheEntry> iterator() {
         return delegate.iterator();
      }

      @SuppressWarnings("ThrowFromFinallyBlock")
      private void run(Runnable runnable, Object key) {
         if (runnable == null) {
            return;
         }
         try {
            clusteringDependentLogic.unlock(key);
            runnable.run();
         } finally {
            try {
               if (!clusteringDependentLogic.lock(key, false)) {
                  throw new RuntimeException("Not locked!");
               }
            } catch (InterruptedException e) {
               Thread.currentThread().interrupt();
               throw new RuntimeException(e);
            }
         }
      }

      @Override
      public <K> void executeTask(KeyFilter<K> filter, KeyValueAction<Object, InternalCacheEntry> action)
            throws InterruptedException {
         throw new NotImplementedException();
      }
   }

   private class AfterPassivationOrCacheWriter extends ControlledCommandInterceptor {

      volatile Runnable beforeEvict;
      volatile Runnable afterEvict;

      public AfterPassivationOrCacheWriter injectThis(Cache<Object, Object> injectInCache) {
         InterceptorChain chain = TestingUtil.extractComponent(injectInCache, InterceptorChain.class);
         if (chain.containsInterceptorType(DistCacheWriterInterceptor.class)) {
            injectInCache.getAdvancedCache().addInterceptorAfter(this, DistCacheWriterInterceptor.class);
         } else if (chain.containsInterceptorType(CacheWriterInterceptor.class)) {
            injectInCache.getAdvancedCache().addInterceptorAfter(this, CacheWriterInterceptor.class);
         } else if (chain.containsInterceptorType(PassivationInterceptor.class)) {
            injectInCache.getAdvancedCache().addInterceptorAfter(this, PassivationInterceptor.class);
         } else {
            throw new IllegalStateException("Should not happen!");
         }
         return this;
      }

      @Override
      public Object visitEvictCommand(InvocationContext ctx, EvictCommand command) throws Throwable {
         return handle(ctx, command, beforeEvict, afterEvict);
      }
   }
}
