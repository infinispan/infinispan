package org.infinispan.eviction.impl;

import static org.infinispan.commons.util.concurrent.CompletionStages.join;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.infinispan.Cache;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.AsyncInterceptor;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.interceptors.impl.EntryWrappingInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntriesEvicted;
import org.infinispan.notifications.cachelistener.event.CacheEntriesEvictedEvent;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.support.WaitNonBlockingStore;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoSchema;
import org.infinispan.test.Mocks;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CheckPoint;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.concurrent.DataOperationOrderer;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

/**
 * Tests size-based eviction with concurrent read and/or write operation. In this test, we have no passivation.
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "eviction.EvictionWithConcurrentOperationsTest")
public class EvictionWithConcurrentOperationsTest extends SingleCacheManagerTest {
   protected boolean passivation = false;
   protected final AtomicInteger storeNamePrefix = new AtomicInteger(0);
   public final String storeName = getClass().getSimpleName();
   protected final String persistentLocation = CommonsTestingUtil.tmpDirectory(getClass());

   public EvictionWithConcurrentOperationsTest() {
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   /**
    * ISPN-3048: this is a simple scenario. a put triggers the eviction while another thread tries to read. the read
    * occurs before the eviction listener is notified (and passivated if enabled). the key still exists in the map
    */
   public void testScenario1() throws Exception {
      final Object key1 = new SameHashCodeKey("key1");
      initializeKeyAndCheckData(key1, "v1");

      final Object key2 = new SameHashCodeKey("key2");
      final Latch latch = new Latch();
      final ControlledPassivationManager controlledPassivationManager = replacePassivationManager(latch);

      //this will trigger the eviction of key1. key1 eviction will be blocked in the latch
      latch.enable();
      Future<Object> put;
      try {
         put = fork(() -> cache.put(key2, "v2"));
         latch.waitToBlock(30, TimeUnit.SECONDS);

         //the eviction was trigger and it blocked before passivation
         assertEquals("Wrong value for key " + key1 + " in get operation.", "v1", cache.get(key1));
      } finally {
         //let the eviction continue and wait for put
         latch.disable();
      }
      put.get(30, TimeUnit.SECONDS);

      assertInMemory(key2, "v2");
      assertNotInMemory(key1, "v1");
   }

   /**
    * ISPN-3048: this is a simple scenario. a put triggers the eviction while another thread tries to read. the read
    * occurs after the eviction listener is notified (if passivation is enabled, it is written to disk). the key still
    * exists in the map
    */
   public void testScenario2() throws Exception {
      final Object key1 = new SameHashCodeKey("key1");
      initializeKeyAndCheckData(key1, "v1");

      final Object key2 = new SameHashCodeKey("key2");
      final Latch latch = new Latch();
      final ControlledPassivationManager controlledPassivationManager = replacePassivationManager(latch);

      //this will trigger the eviction of key1. key1 eviction will be blocked in the latch
      latch.enable();
      Future<Object> put;
      try {
         put = fork(() -> cache.put(key2, "v2"));
         latch.waitToBlock(30, TimeUnit.SECONDS);

         //the eviction was trigger and it blocked before passivation
         assertEquals("Wrong value for key " + key1 + " in get operation.", "v1", cache.get(key1));
      } finally {
         //let the eviction continue and wait for put
         latch.disable();
      }
      put.get(30, TimeUnit.SECONDS);

      assertInMemory(key2, "v2");
      assertNotInMemory(key1, "v1");
   }

   /**
    * ISPN-3048: a put triggers the eviction while another thread tries to read. the read occurs after the eviction
    * listener is notified (if passivation is enabled, it is written to disk) and after the entry is removed from the
    * map. however, it should be able to load it from persistence.
    */
   public void testScenario3() throws Exception {
      final Object key1 = new SameHashCodeKey("key1");
      initializeKeyAndCheckData(key1, "v1");

      final Object key2 = new SameHashCodeKey("key2");
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
      Future<Object> put;
      try {
         put = fork(() -> cache.put(key2, "v2"));
         latch.waitToBlock(30, TimeUnit.SECONDS);
      } finally {
         //let the eviction continue and wait for put
         latch.disable();
      }
      put.get(30, TimeUnit.SECONDS);

      //the eviction was trigger and the key is no longer in the map
      // This should be after the async put is known to finish.  It is undefined which would
      // win in the case of an entry being activated while it is also being passivated
      // This way it is clear which should be there
      assertEquals("Wrong value for key " + key1 + " in get operation.", "v1", cache.get(key1));

      assertInMemory(key1, "v1");
      assertNotInMemory(key2, "v2");
   }

   /**
    * ISPN-3048: a put triggers the eviction while another thread tries to read. the read occurs after the eviction
    * listener is notified (if passivation is enabled, it is written to disk) and after the entry is removed from the
    * map. however, a concurrent read happens at the same time before the first has time to load it from persistence.
    */
   public void testScenario4() throws Exception {
      final Object key1 = new SameHashCodeKey("key1");
      initializeKeyAndCheckData(key1, "v1");

      final Object key2 = new SameHashCodeKey("key2");
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
      Future<Object> put = fork(() -> cache.put(key2, "v2"));
      writeLatch.waitToBlock(30, TimeUnit.SECONDS);

      //the eviction was trigger and the key is no longer in the map
      Future<Object> get = fork(() -> cache.get(key1));
      readLatch.waitToBlock(30, TimeUnit.SECONDS);

      // Ensures the eviction is complete of key1
      put.get(30, TimeUnit.SECONDS);

      //the first read is blocked. it has check the data container and it didn't found any value
      //this second get should not block anywhere and it should fetch the value from persistence
      assertEquals("Wrong value for key " + key1 + " in get operation.", "v1", cache.get(key1));

      //let the second get continue
      readLatch.disable();
      assertEquals("Wrong value for key " + key1 + " in get operation.", "v1", get.get());

      assertInMemory(key1, "v1");
      assertNotInMemory(key2, "v2");
   }

   /**
    * ISPN-3048: a put triggers the eviction while another thread tries to read. the read occurs after the eviction
    * listener is notified (if passivation is enabled, it is written to disk) and after the entry is removed from the
    * map. however, a concurrent put happens at the same time before the get has time to load it from persistence.
    */
   public void testScenario5() throws Exception {
      final Object key1 = new SameHashCodeKey("key1");
      initializeKeyAndCheckData(key1, "v1");

      final Object key2 = new SameHashCodeKey("key2");
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
      Future<Object> get;
      try {
         cache.put(key2, "v2");
         writeLatch.waitToBlock(30, TimeUnit.SECONDS);

         //the eviction was trigger and the key is no longer in the map
         get = fork(() -> cache.get(key1));
         readLatch.waitToBlock(30, TimeUnit.SECONDS);

         //the first read is blocked. it has check the data container and it didn't found any value
         //this second get should not block anywhere and it should fetch the value from persistence
         assertEquals("Wrong value for key " + key1 + " in get operation.", "v1", cache.put(key1, "v3"));
      } finally {

         //let the get continue
         readLatch.disable();
      }
      assertEquals("Wrong value for key " + key1 + " in get operation.", "v3", get.get(30, TimeUnit.SECONDS));

      assertInMemory(key1, "v3");
      assertNotInMemory(key2, "v2");
   }

   /**
    * ISPN-3048: a put triggers the eviction while another thread tries to read. the read occurs after the eviction
    * listener is notified (if passivation is enabled, it is written to disk) and after the entry is removed from the
    * map. however, a concurrent put happens at the same time before the get has time to load it from persistence. The
    * get will occur after the put writes to persistence and before writes to data container
    */
   public void testScenario6() throws Exception {
      final Object key1 = new SameHashCodeKey("key1");
      initializeKeyAndCheckData(key1, "v1");

      final Object key2 = new SameHashCodeKey("key2");
      final Latch readLatch = new Latch();
      final Latch writeLatch = new Latch();
      final Latch writeLatch2 = new Latch();
      final AtomicBoolean firstWriter = new AtomicBoolean(false);
      final AfterEntryWrappingInterceptor afterEntryWrappingInterceptor = new AfterEntryWrappingInterceptor()
            .injectThis(cache);
      afterEntryWrappingInterceptor.beforeGet = () -> readLatch.blockIfNeeded();
      afterEntryWrappingInterceptor.afterPut = () -> {
         if (!firstWriter.compareAndSet(false, true)) {
            writeLatch2.blockIfNeeded();
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
      Future<Object> get;
      Future<Object> put2;
      try {
         Future<Object> put = fork(() -> cache.put(key2, "v2"));
         writeLatch.waitToBlock(30, TimeUnit.SECONDS);

         //the eviction was trigger and the key is no longer in the map
         get = fork(() -> cache.get(key1));
         readLatch.waitToBlock(30, TimeUnit.SECONDS);

         // Ensures the eviction is complete of key1
         put.get(30, TimeUnit.SECONDS);

         put2 = cache.putAsync(key1, "v3");

         //wait until the 2nd put writes to persistence
         writeLatch2.waitToBlock(30, TimeUnit.SECONDS);
      } finally {
         //let the get continue
         readLatch.disable();
      }
      assertPossibleValues(key1, get.get(30, TimeUnit.SECONDS), "v1", "v3");

      assertEquals("Wrong value for key " + key1 + " in put operation.", "v1", put2.get(30, TimeUnit.SECONDS));

      assertInMemory(key1, "v3");
      assertNotInMemory(key2, "v2");
   }

   /**
    * ISPN-3854: an entry is evicted. a get operation is performed and loads the entry from the persistence. However,
    * before continue the processing, a put succeeds and updates the key. Check in the end if the key is correctly
    * stored or not.
    */
   public void testScenario7() throws Exception {
      final Object key1 = new SameHashCodeKey("key1");
      initializeKeyAndCheckData(key1, "v1");

      final Latch readLatch = new Latch();
      final AfterActivationOrCacheLoader commandController = new AfterActivationOrCacheLoader()
            .injectThis(cache);
      commandController.afterGet = () -> readLatch.blockIfNeeded();

      cache.evict(key1);

      assertNotInMemory(key1, "v1");

      //perform the get. it will load the entry from cache loader.
      readLatch.enable();
      Future<Object> get;
      try {
         get = fork(() -> cache.get(key1));
         readLatch.waitToBlock(30, TimeUnit.SECONDS);

         //now, we perform a put.
         assertEquals("Wrong value for key " + key1 + " in put operation.", "v1", cache.put(key1, "v2"));
      } finally {
         //we let the get go...
         readLatch.disable();
      }
      assertPossibleValues(key1, get.get(30, TimeUnit.SECONDS), "v1");

      assertInMemory(key1, "v2");
   }

   // Data is written to container but before releasing orderer the entry is evicted. In this case the entry
   // should be passivated to ensure data is still around
   public void testEvictionDuringWrite() throws InterruptedException, ExecutionException, TimeoutException {
      String key = "evicted-key";
      // We use skip cache load to prevent the additional loading of the entry in the initial write
      testEvictionDuring(key, () -> cache.getAdvancedCache().withFlags(Flag.SKIP_CACHE_LOAD).put(key, "value"),
            AssertJUnit::assertNull, AssertJUnit::assertNotNull, true);
   }

   // This case the removal acquires the orderer lock, but doesn't yet remove the value. Then the eviction occurs
   // but can't complete yet as the orderer is still owned by removal. Then remove completes, but the eviction
   // should <b>NOT</b> passivate the entry as it was removed prior.
   public void testEvictionDuringRemove() throws InterruptedException, ExecutionException, TimeoutException {
      String key = "evicted-key";
      cache.put(key, "removed");
      testEvictionDuring(key, () -> cache.remove(key), AssertJUnit::assertNotNull, AssertJUnit::assertNull, false);
   }

   /**
    * Tests that an entry was written to container, but before it releases its orderer it is evicted
    */
   void testEvictionDuring(String key, Callable<Object> callable, Consumer<Object> valueConsumer,
         Consumer<Object> finalResultConsumer, boolean blockOnCompletion) throws TimeoutException, InterruptedException, ExecutionException {
      // We use this checkpoint to hold the orderer lock during the write - which means the eviction will have to handle
      // it appropriately
      CheckPoint operationCheckPoint = new CheckPoint("operation");
      operationCheckPoint.triggerForever(blockOnCompletion ? Mocks.AFTER_RELEASE : Mocks.BEFORE_RELEASE);

      DataOperationOrderer original;
      if (blockOnCompletion) {
         // Blocks just before releasing the orderer
         original = Mocks.blockingMock(operationCheckPoint, DataOperationOrderer.class, cache,
               (stub, m) -> stub.when(m).completeOperation(eq(key), any(), any()));
      } else {
         // Blocks just after acquiring orderer
         original = Mocks.blockingMock(operationCheckPoint, DataOperationOrderer.class, cache,
               (stub, m) -> stub.when(m).orderOn(eq(key), any()));
      }

      // Put the key which will wait on releasing the orderer at the end
      Future<Object> operationFuture = fork(callable);
      // Confirm everything is complete except releasing orderer
      operationCheckPoint.awaitStrict(Mocks.BEFORE_INVOCATION, 10, TimeUnit.SECONDS);

      // Replace the original so the eviction doesn't get blocked by the other check point
      TestingUtil.replaceComponent(cache, DataOperationOrderer.class, original, true);

      // We use this checkpoint to wait until the eviction is in process (that is that it has the caffeine lock
      // and has to wait until the prior orderer above completes
      CheckPoint evictionCheckPoint = new CheckPoint("eviction");
      evictionCheckPoint.triggerForever(Mocks.BEFORE_RELEASE);

      Mocks.blockingMock(evictionCheckPoint, DataOperationOrderer.class, cache,
                         (stub, m) -> stub.when(m).orderOn(eq(key), any()));

      // Put another key, which will evict our original key
      Future<Object> evictFuture = fork(() -> cache.put("other-key", "other-value"));

      // Now wait for the eviction to retrieve the orderer - but don't let it continue
      evictionCheckPoint.awaitStrict(Mocks.AFTER_INVOCATION, 10, TimeUnit.SECONDS);

      // Let the put complete
      operationCheckPoint.trigger(blockOnCompletion ? Mocks.BEFORE_RELEASE : Mocks.AFTER_RELEASE);

      // If the block is not on completion, that means that eviction holds the caffeine lock which means it may
      // be preventing the actual operation from completing - thus we free the eviction sooner
      if (!blockOnCompletion) {
         evictionCheckPoint.triggerForever(Mocks.AFTER_RELEASE);
      }

      // And ensure the operation complete
      valueConsumer.accept(operationFuture.get(10, TimeUnit.SECONDS));

      // Finally let the eviction to complete if it wasn't above
      evictionCheckPoint.triggerForever(Mocks.AFTER_RELEASE);

      evictFuture.get(10, TimeUnit.SECONDS);

      finalResultConsumer.accept(cache.get(key));
   }

   protected void initializeKeyAndCheckData(Object key, Object value) {
      assertTrue("A cache store should be configured!", cache.getCacheConfiguration().persistence().usingStores());
      cache.put(key, value);
      DataContainer<?, ?> container = cache.getAdvancedCache().getDataContainer();
      InternalCacheEntry<?, ?> entry = container.peek(key);
      assertNotNull("Key " + key + " does not exist in data container.", entry);
      assertEquals("Wrong value for key " + key + " in data container.", value, entry.getValue());

      WaitNonBlockingStore<?, ?> loader = TestingUtil.getFirstStoreWait(cache);
      MarshallableEntry<?, ?> entryLoaded = loader.loadEntry(key);
      if (passivation) {
         assertNull(entryLoaded);
      } else {
         assertEquals("Wrong value for key " + key + " in cache loader", value, extractValue(entryLoaded));
      }
   }

   protected void assertInMemory(Object key, Object value) {
      DataContainer<?, ?> container = cache.getAdvancedCache().getDataContainer();
      InternalCacheEntry<?, ?> entry = container.peek(key);
      assertNotNull("Key " + key + " does not exist in data container", entry);
      assertEquals("Wrong value for key " + key + " in data container", value, entry.getValue());

      WaitNonBlockingStore<?, ?> loader = TestingUtil.getFirstStoreWait(cache);
      // The entry may or may not be on disk with passivation
      if (!passivation) {
         MarshallableEntry<?, ?> entryLoaded = loader.loadEntry(key);
         assertEquals("Wrong value for key " + key + " in cache loader", value, extractValue(entryLoaded));
      }
   }

   protected void assertNotInMemory(Object key, Object value) {
      DataContainer<?, ?> container = cache.getAdvancedCache().getDataContainer();
      InternalCacheEntry<?, ?> memoryEntry = container.peek(key);
      assertNull("Key " + key + " exists in data container", memoryEntry);

      WaitNonBlockingStore<?, ?> loader = TestingUtil.getFirstStoreWait(cache);
      if (passivation) {
         // With passivation the store write is sometimes delayed
         PersistenceManager pm = TestingUtil.extractComponent(cache, PersistenceManager.class);
         MarshallableEntry<?, ?> entryLoaded = join(pm.loadFromAllStores(key, true, true));
         assertEquals("Wrong value for key " + key + " in cache loader", value, extractValue(entryLoaded));

         eventuallyEquals("Wrong value for key " + key + " in cache loader",
                          value, () -> extractValue(loader.loadEntry(key)));
      } else {
         MarshallableEntry<?, ?> entryLoaded = loader.loadEntry(key);
         assertEquals("Wrong value for key " + key + " in cache loader", value, extractValue(entryLoaded));
      }
   }

   private Object extractValue(MarshallableEntry<?, ?> entry) {
      return entry != null ? entry.getValue() : null;
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = getDefaultStandaloneCacheConfig(false);
      configurePersistence(builder);
      configureEviction(builder);
      GlobalConfigurationBuilder globalBuilder = new GlobalConfigurationBuilder().nonClusteredDefault();
      globalBuilder.serialization().addContextInitializer(new EvictionWithConcurrentOperationsSCIImpl());
      globalBuilder.globalState().enable().persistentLocation(persistentLocation);
      return TestCacheManagerFactory.createCacheManager(globalBuilder, builder);
   }

   @AfterClass(alwaysRun = true)
   protected void clearTempDir() {
      Util.recursiveFileRemove(persistentLocation);
   }

   protected void configureEviction(ConfigurationBuilder builder) {
      builder.memory().size(1);
   }

   protected void configurePersistence(ConfigurationBuilder builder) {
      builder.persistence().passivation(false).addStore(DummyInMemoryStoreConfigurationBuilder.class)
            .storeName(storeName + storeNamePrefix.getAndIncrement());
   }

   protected final ControlledPassivationManager replacePassivationManager(final Latch latch) {
      PassivationManager current = TestingUtil.extractComponent(cache, PassivationManager.class);
      ControlledPassivationManager controlledPassivationManager = new ControlledPassivationManager(current);
      controlledPassivationManager.beforePassivate = latch::blockIfNeeded;
      TestingUtil.replaceComponent(cache, PassivationManager.class, controlledPassivationManager, true);
      return controlledPassivationManager;
   }

   protected void assertPossibleValues(Object key, Object value, Object... expectedValues) {
      for (Object expectedValue : expectedValues) {
         if (value == null ? expectedValue == null : value.equals(expectedValue)) {
            return;
         }
      }
      fail("Wrong value for key " + key + ". value=" + value + ", expectedValues=" +
           Arrays.toString(expectedValues));
   }

   public static class SameHashCodeKey {

      @ProtoField(1)
      final String name;

      @ProtoField(number = 2, defaultValue = "0")
      final int hashCode;

      //same hash code to force the keys to be in the same segment.
      SameHashCodeKey(String name) {
         this(name, 0);
      }

      @ProtoFactory
      SameHashCodeKey(String name, int hashCode) {
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

   protected static class ControlledPassivationManager implements PassivationManager {
      protected final PassivationManager delegate;
      protected volatile Runnable beforePassivate;
      protected volatile Runnable afterPassivate;

      private ControlledPassivationManager(PassivationManager delegate) {
         this.delegate = delegate;
      }

      @Override
      public boolean isEnabled() {
         return delegate.isEnabled();
      }

      @Override
      public CompletionStage<Void> passivateAsync(InternalCacheEntry entry) {
         final Runnable before = beforePassivate;
         if (before != null) {
            before.run();
         }

         CompletionStage<Void> stage = delegate.passivateAsync(entry);
         final Runnable after = afterPassivate;
         if (after != null) {
            return stage.thenRun(after);
         }
         return stage;
      }

      @Override
      public CompletionStage<Void> passivateAllAsync() {
         return delegate.passivateAllAsync();
      }

      @Override
      public void skipPassivationOnStop(boolean skip) {
         delegate.skipPassivationOnStop(skip);
      }

      @Override
      public long getPassivations() {
         return delegate.getPassivations();
      }

      @Override
      public boolean getStatisticsEnabled() {
         return delegate.getStatisticsEnabled();
      }

      @Override
      public void setStatisticsEnabled(boolean enabled) {
         delegate.setStatisticsEnabled(enabled);
      }

      @Override
      public void resetStatistics() {
         delegate.resetStatistics();
      }
   }

   @Listener(sync = true)
   protected abstract static class SyncEvictionListener {
      @CacheEntriesEvicted
      public abstract void evicted(CacheEntriesEvictedEvent event);
   }

   protected abstract static class ControlledCommandInterceptor extends DDAsyncInterceptor {
      volatile Runnable beforeGet;
      volatile Runnable afterGet;
      volatile Runnable beforePut;
      volatile Runnable afterPut;

      @Override
      public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
         return handle(ctx, command, beforePut, afterPut);
      }

      @Override
      public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
         return handle(ctx, command, beforeGet, afterGet);
      }

      @Override
      public Object visitGetCacheEntryCommand(InvocationContext ctx, GetCacheEntryCommand command) throws Throwable {
         return handle(ctx, command, beforeGet, afterGet);
      }

      protected final Object handle(InvocationContext ctx, VisitableCommand command, Runnable before, Runnable after)
            throws Throwable {
         if (before != null) {
            before.run();
         }
         return invokeNextThenAccept(ctx, command, (rCtx, rCommand, rv) -> {
            if (after != null) {
               after.run();
            }
         });
      }
   }

   protected class AfterEntryWrappingInterceptor extends ControlledCommandInterceptor {

      public AfterEntryWrappingInterceptor injectThis(Cache<Object, Object> injectInCache) {
         TestingUtil.extractInterceptorChain(injectInCache).addInterceptorAfter(this, EntryWrappingInterceptor.class);
         return this;
      }

   }

   class AfterActivationOrCacheLoader extends ControlledCommandInterceptor {

      public AfterActivationOrCacheLoader injectThis(Cache<Object, Object> injectInCache) {
         AsyncInterceptorChain chain =
               TestingUtil.extractComponent(injectInCache, AsyncInterceptorChain.class);
         AsyncInterceptor loaderInterceptor =
               chain.findInterceptorExtending(org.infinispan.interceptors.impl.CacheLoaderInterceptor.class);
         TestingUtil.extractInterceptorChain(injectInCache).addInterceptorAfter(this, loaderInterceptor.getClass());
         return this;
      }

   }

   protected static class Latch {

      private boolean enabled = false;
      private boolean blocked = false;

      public final synchronized void enable() {
         this.enabled = true;
      }

      public final synchronized void disable() {
         this.enabled = false;
         notifyAll();
      }

      public final synchronized void blockIfNeeded() {
         blocked = true;
         notifyAll();
         while (enabled) {
            try {
               wait(TimeUnit.SECONDS.toMillis(10));
            } catch (InterruptedException e) {
               Thread.currentThread().interrupt();
               return;
            }
         }
      }

      public final synchronized void waitToBlock(long timeout, TimeUnit timeUnit) throws InterruptedException, TimeoutException {
         final long endTime = Util.currentMillisFromNanotime() + timeUnit.toMillis(timeout);
         long waitingTime;
         while (!blocked && (waitingTime = endTime - Util.currentMillisFromNanotime()) > 0) {
            wait(waitingTime);
         }
         if (!blocked) {
            throw new TimeoutException();
         }

      }
   }

   @ProtoSchema(
         includeClasses = SameHashCodeKey.class,
         schemaFileName = "test.core.eviction.proto",
         schemaFilePath = "proto/generated",
         schemaPackageName = "org.infinispan.test.core.eviction",
         service = false
   )
   interface EvictionWithConcurrentOperationsSCI extends SerializationContextInitializer {
   }
}
