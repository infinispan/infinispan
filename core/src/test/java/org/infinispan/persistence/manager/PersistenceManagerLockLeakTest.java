package org.infinispan.persistence.manager;

import static org.infinispan.commons.util.concurrent.CompletionStages.join;
import static org.infinispan.persistence.manager.PersistenceManager.AccessMode.BOTH;
import static org.infinispan.test.TestingUtil.extractComponent;
import static org.infinispan.test.TestingUtil.getFirstStore;
import static org.testng.AssertJUnit.assertTrue;

import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.persistence.impl.MarshalledEntryUtil;
import org.infinispan.persistence.dummy.DummyInMemoryStore;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfiguration;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.reactivestreams.Publisher;
import org.testng.annotations.Test;

import io.reactivex.rxjava3.core.Flowable;

@CleanupAfterMethod
@Test(groups = "unit", testName = "persistence.manager.PersistenceManagerLockLeakTest")
public class PersistenceManagerLockLeakTest extends SingleCacheManagerTest {

   private static final int STOP_TIMEOUT_SECONDS = 10;

   @Override
   protected EmbeddedCacheManager createCacheManager() {
      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(false);
      cfg.persistence().addStore(ControllableStore.ConfigurationBuilder.class);
      return TestCacheManagerFactory.createCacheManager(cfg);
   }

   public void testStopCompletesWhenRemoveSegmentsStageAbandoned() throws Exception {
      ControllableStore store = getFirstStore(cache);
      store.delayNextOperation();

      persistenceManager().removeSegments(IntSets.immutableSet(0));

      assertStopCompletes(store);
   }

   public void testStopCompletesWhenAddSegmentsStageAbandoned() throws Exception {
      ControllableStore store = getFirstStore(cache);
      store.delayNextOperation();

      persistenceManager().addSegments(IntSets.immutableSet(0));

      assertStopCompletes(store);
   }

   public void testStopCompletesWhenWriteStageAbandoned() throws Exception {
      ControllableStore store = getFirstStore(cache);
      KeyPartitioner keyPartitioner = extractComponent(cache, KeyPartitioner.class);
      store.delayNextOperation();

      String key = "k";
      persistenceManager().writeToAllNonTxStores(
            MarshalledEntryUtil.create(key, "v", cache), keyPartitioner.getSegment(key), BOTH);

      assertStopCompletes(store);
   }

   public void testStopCompletesWhenDeleteStageAbandoned() throws Exception {
      ControllableStore store = getFirstStore(cache);
      KeyPartitioner keyPartitioner = extractComponent(cache, KeyPartitioner.class);

      String key = "k";
      int segment = keyPartitioner.getSegment(key);
      join(persistenceManager().writeToAllNonTxStores(
            MarshalledEntryUtil.create(key, "v", cache), segment, BOTH));

      store.delayNextOperation();
      persistenceManager().deleteFromAllStores(key, segment, BOTH);

      assertStopCompletes(store);
   }

   public void testStopCompletesWhenClearStageAbandoned() throws Exception {
      ControllableStore store = getFirstStore(cache);
      store.delayNextOperation();

      persistenceManager().clearAllStores(BOTH);

      assertStopCompletes(store);
   }

   public void testStopCompletesWhenLoadStageAbandoned() throws Exception {
      ControllableStore store = getFirstStore(cache);
      store.delayNextOperation();

      persistenceManager().loadFromAllStores("k", false, true);

      assertStopCompletes(store);
   }

   public void testStopCompletesWhenSizeStageAbandoned() throws Exception {
      ControllableStore store = getFirstStore(cache);
      store.delayNextOperation();

      persistenceManager().size(BOTH, IntSets.immutableRangeSet(256));

      assertStopCompletes(store);
   }

   public void testStopCompletesWhenApproximateSizeStageAbandoned() throws Exception {
      ControllableStore store = getFirstStore(cache);
      store.delayNextOperation();

      persistenceManager().approximateSize(BOTH, IntSets.immutableRangeSet(256));

      assertStopCompletes(store);
   }

   public void testStopCompletesWhenPurgeExpiredStageAbandoned() throws Exception {
      ControllableStore store = getFirstStore(cache);
      store.delayNextOperation();

      persistenceManager().purgeExpired();

      assertStopCompletes(store);
   }

   private PersistenceManager persistenceManager() {
      return extractComponent(cache, PersistenceManager.class);
   }

   private void assertStopCompletes(ControllableStore store) throws Exception {
      PersistenceManagerImpl pm = (PersistenceManagerImpl) persistenceManager();
      assertTrue("Read lock should be held while stage is pending", pm.anyLocksHeld());

      Future<Void> stopFuture = fork(pm::stop);
      try {
         stopFuture.get(STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      } finally {
         store.endDelay();
         if (!stopFuture.isDone()) {
            stopFuture.get(30, TimeUnit.SECONDS);
         }
      }
   }

   /**
    * A store that is {@link Characteristic#SEGMENTABLE} but NOT {@link Characteristic#SHAREABLE}, so {@link PersistenceManagerImpl}
    * invokes segment methods through its read-lock-guarded path.
    *
    * <p>
    * Any store operation can be made to return a pending {@link CompletableFuture} via {@link #delayNextOperation()},
    * keeping the persistence manager's read lock held.
    * </p>
    */
   public static class ControllableStore extends DummyInMemoryStore<Object, Object> {
      private volatile CompletableFuture<Void> delayFuture = CompletableFutures.completedNull();

      public void delayNextOperation() {
         delayFuture = new CompletableFuture<>();
      }

      public void endDelay() {
         CompletableFuture<Void> old = delayFuture;
         delayFuture = CompletableFutures.completedNull();
         old.complete(null);
      }

      private boolean shouldDelay() {
         return !delayFuture.isDone();
      }

      @Override
      public Set<Characteristic> characteristics() {
         return EnumSet.of(Characteristic.BULK_READ, Characteristic.EXPIRATION, Characteristic.SEGMENTABLE);
      }

      @Override
      public CompletionStage<Void> removeSegments(IntSet segments) {
         if (shouldDelay()) return delayFuture;
         return super.removeSegments(segments);
      }

      @Override
      public CompletionStage<Void> addSegments(IntSet segments) {
         if (shouldDelay()) return delayFuture;
         return super.addSegments(segments);
      }

      @Override
      public CompletionStage<Void> write(int segment, MarshallableEntry entry) {
         if (shouldDelay()) return delayFuture;
         return super.write(segment, entry);
      }

      @Override
      public CompletionStage<Boolean> delete(int segment, Object key) {
         if (shouldDelay()) return delayFuture.thenApply(__ -> true);
         return super.delete(segment, key);
      }

      @Override
      public CompletionStage<Void> clear() {
         if (shouldDelay()) return delayFuture;
         return super.clear();
      }

      @Override
      public CompletionStage<MarshallableEntry<Object, Object>> load(int segment, Object key) {
         if (shouldDelay()) return delayFuture.thenApply(__ -> null);
         return super.load(segment, key);
      }

      @Override
      public CompletionStage<Long> size(IntSet segments) {
         if (shouldDelay()) return delayFuture.thenApply(__ -> 0L);
         return super.size(segments);
      }

      @Override
      public CompletionStage<Long> approximateSize(IntSet segments) {
         if (shouldDelay()) return delayFuture.thenApply(__ -> 0L);
         return super.approximateSize(segments);
      }

      @Override
      public Publisher<MarshallableEntry<Object, Object>> purgeExpired() {
         if (shouldDelay()) {
            return Flowable.fromCompletionStage(delayFuture)
                  .flatMap(__ -> Flowable.empty());
         }
         return super.purgeExpired();
      }

      @BuiltBy(ConfigurationBuilder.class)
      @ConfigurationFor(ControllableStore.class)
      public static class Configuration extends DummyInMemoryStoreConfiguration {

         public Configuration(AttributeSet attributes, AsyncStoreConfiguration async) {
            super(attributes, async);
         }
      }

      public static class ConfigurationBuilder extends DummyInMemoryStoreConfigurationBuilder {

         public ConfigurationBuilder(PersistenceConfigurationBuilder builder) {
            super(builder);
         }

         @Override
         public Configuration create() {
            return new Configuration(attributes.protect(), async.create());
         }
      }
   }
}
