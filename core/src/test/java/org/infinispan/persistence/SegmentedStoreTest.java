package org.infinispan.persistence;

import static org.testng.Assert.assertEquals;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;
import java.util.function.ToIntFunction;

import org.infinispan.Cache;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.core.MarshalledEntryImpl;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.manager.PersistenceManagerImpl;
import org.infinispan.persistence.spi.SegmentedAdvancedLoadWriteStore;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.reactivestreams.Publisher;
import org.testng.annotations.Test;

import io.reactivex.Flowable;

/**
 * Test that ensures that various segment based methods work properly
 * @author William Burns
 * @since 9.4
 */
@Test (groups = "functional", testName = "persistence.SegmentedStoreTest")
public abstract class SegmentedStoreTest extends SingleCacheManagerTest {

   private static final int NUM_ENTRIES = 100;

   protected SegmentedAdvancedLoadWriteStore<Object, Object> store;
   protected Cache<Object, Object> cache;
   protected StreamingMarshaller sm;
   protected Set<Integer>[] keys;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cb = getDefaultStandaloneCacheConfig(false);
      configurePersistence(cb);
      EmbeddedCacheManager manager = TestCacheManagerFactory.createCacheManager(cb);
      cache = manager.getCache();
      ComponentRegistry componentRegistry = cache.getAdvancedCache().getComponentRegistry();
      PersistenceManagerImpl pm = (PersistenceManagerImpl) componentRegistry.getComponent(PersistenceManager.class);
      sm = pm.getMarshaller();
      store = TestingUtil.getFirstLoader(cache);
      keys = new Set[cache.getCacheConfiguration().clustering().hash().numSegments()];
      return manager;
   }

   protected abstract void configurePersistence(ConfigurationBuilder cb);

   public void testSize() {
      runTest(is -> store.size(is));
   }

   public void testIterationWithKeys() {
      runTest(intSetFunctionFromIntSetPublisherFunction(is -> store.publishKeys(is, null)));
   }

   public void testIterationWithValueAndMetadata() {
      runTest(intSetFunctionFromIntSetPublisherFunction(is -> store.publishEntries(is, null, true, true)));
   }

   public void testIterationWithValueWithoutMetadata() {
      runTest(intSetFunctionFromIntSetPublisherFunction(is -> store.publishEntries(is, null, true, false)));
   }

   public void testIterationWithoutValueWithMetadata() {
      runTest(intSetFunctionFromIntSetPublisherFunction(is -> store.publishEntries(is, null, false, true)));
   }

   public void testIterationWithoutValueOrMetadata() {
      runTest(intSetFunctionFromIntSetPublisherFunction(is -> store.publishEntries(is, null, false, false)));
   }

   // Provides a function that counts how many objects are in the returned publisher for the given IntSet
   // This is useful to abstract how the Publisher is created without duplicating code
   private ToIntFunction<IntSet> intSetFunctionFromIntSetPublisherFunction(Function<IntSet, Publisher<?>> intSetToPublisher) {
      return is -> Flowable.fromPublisher(intSetToPublisher.apply(is))
            .count().blockingGet().intValue();
   }

   private void runTest(ToIntFunction<IntSet> intSetIntFunction) {
      insertData();
      IntSet mutableSet = IntSets.mutableSet(8);
      int expected = 0;
      for (int segment : new int[] {3, 5, 23, 94, 103, 183, 201, 213}) {
         mutableSet.set(segment);
         Set<Integer> set = keys[segment];
         if (set != null) {
            expected += set.size();
         }
      }

      assertEquals(expected, intSetIntFunction.applyAsInt(mutableSet));
   }

   private void insertData() {
      KeyPartitioner keyPartitioner = TestingUtil.extractComponent(cache, KeyPartitioner.class);
      for (int i = 0; i < NUM_ENTRIES; i++) {
         int segment = keyPartitioner.getSegment(i);
         Set<Integer> keysForSegment = keys[segment];
         if (keysForSegment == null) {
            keysForSegment = new HashSet<>();
            keys[segment] = keysForSegment;
         }
         keysForSegment.add(i);

         MarshalledEntryImpl me = new MarshalledEntryImpl<>(i, i, null, sm);
         store.write(me);
      }
   }
}
