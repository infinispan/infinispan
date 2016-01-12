package org.infinispan.eviction.impl;

import org.infinispan.Cache;
import org.infinispan.commands.write.EvictCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.filter.KeyFilter;
import org.infinispan.filter.KeyValueFilter;
import org.infinispan.interceptors.CacheLoaderInterceptor;
import org.infinispan.interceptors.CacheWriterInterceptor;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.annotation.CacheEntriesEvicted;
import org.infinispan.notifications.cachelistener.event.CacheEntriesEvictedEvent;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
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
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

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
      controller.beforeEvict.set(() -> latch.blockIfNeeded());

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
      replaceControlledDataContainer(latch);

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
      afterEntryWrappingInterceptor.beforeGet.set(() -> {
         if (firstGet.compareAndSet(false, true)) {
            readLatch.blockIfNeeded();
         }
      });
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
      afterEntryWrappingInterceptor.beforeGet.set(() -> readLatch.blockIfNeeded());
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
      afterEntryWrappingInterceptor.beforeGet.set(() -> readLatch.blockIfNeeded());
      afterEntryWrappingInterceptor.afterPut.set(() -> writeLatch2.blockIfNeeded());
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
      //noinspection unchecked
      ControlledDataContainer controlledDataContainer = new ControlledDataContainer(current);
      controlledDataContainer.beforeEvict = new Runnable() {
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

   private class ControlledDataContainer<K, V> implements DataContainer<K, V> {

      private final DataContainer<K, V> delegate;
      private volatile Runnable beforeEvict;

      private ControlledDataContainer(DataContainer<K, V> delegate) {
         this.delegate = delegate;
      }

      @Override
      public InternalCacheEntry<K, V> get(Object k) {
         return delegate.get(k);
      }

      @Override
      public InternalCacheEntry<K, V> peek(Object k) {
         return delegate.peek(k);
      }

      @Override
      public void put(K k, V v, Metadata metadata) {
         delegate.put(k, v, metadata);
      }

      @Override
      public boolean containsKey(Object k) {
         return delegate.containsKey(k);
      }

      @Override
      public InternalCacheEntry<K, V> remove(Object k) {
         return delegate.remove(k);
      }

      @Override
      public int size() {
         return delegate.size();
      }

      @Override
      public int sizeIncludingExpired() {
         return delegate.sizeIncludingExpired();
      }

      @Override
      @Stop(priority = 999)
      public void clear() {
         delegate.clear();
      }

      @Override
      public Set<K> keySet() {
         return delegate.keySet();
      }

      @Override
      public Collection<V> values() {
         return delegate.values();
      }

      @Override
      public Set<InternalCacheEntry<K, V>> entrySet() {
         return delegate.entrySet();
      }

      @Override
      public void purgeExpired() {
         delegate.purgeExpired();
      }

      @Override
      public void evict(K key) {
         run(beforeEvict);
         delegate.evict(key);
      }

      @Override
      public InternalCacheEntry<K, V> compute(K key, ComputeAction<K, V> action) {
         return delegate.compute(key, action);
      }

      @Override
      public Iterator<InternalCacheEntry<K, V>> iterator() {
         return delegate.iterator();
      }

      @Override
      public Iterator<InternalCacheEntry<K, V>> iteratorIncludingExpired() {
         return delegate.iteratorIncludingExpired();
      }

      @Override
      public void executeTask(KeyFilter<? super K> filter, BiConsumer<? super K, InternalCacheEntry< K, V>> action)
            throws InterruptedException {
         throw new NotImplementedException();
      }

      @Override
      public void executeTask(KeyValueFilter<? super K, ? super V> filter, BiConsumer<? super K, InternalCacheEntry<K, V>> action) throws InterruptedException {
         throw new NotImplementedException();
      }

      private void run(Runnable runnable) {
         if (runnable == null) {
            return;
         }
         runnable.run();
      }
   }

   private class AfterPassivationOrCacheWriter extends ControlledCommandInterceptor {

      AtomicReference<Runnable> beforeEvict = new AtomicReference<>();
      AtomicReference<Runnable> afterEvict = new AtomicReference<>();

      public AfterPassivationOrCacheWriter injectThis(Cache<Object, Object> injectInCache) {
         InterceptorChain chain = TestingUtil.extractComponent(injectInCache, InterceptorChain.class);
         List<CommandInterceptor> list = chain.getInterceptorsWhichExtend(CacheWriterInterceptor.class);
         if (list.isEmpty()) {
            list = chain.getInterceptorsWhichExtend(CacheLoaderInterceptor.class);
         }
         if (list.isEmpty()) {
            throw new IllegalStateException("Should not happen!");
         }
         injectInCache.getAdvancedCache().addInterceptorAfter(this, list.get(0).getClass());
         return this;
      }

      @Override
      public Object visitEvictCommand(InvocationContext ctx, EvictCommand command) throws Throwable {
         return handle(ctx, command, beforeEvict.get(), afterEvict.get());
      }
   }
}
