package org.infinispan.persistence;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.persistence.impl.MarshalledEntryUtil;
import org.infinispan.metadata.Metadata;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.support.WaitNonBlockingStore;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.concurrent.WithinThreadExecutor;
import org.testng.annotations.Test;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.subscribers.TestSubscriber;


/**
 * @author Mircea Markus
 * @since 6.0
 */
@Test (groups = "functional", testName = "persistence.ParallelIterationTest")
public abstract class ParallelIterationTest extends SingleCacheManagerTest {

   private static final int NUM_THREADS = 10;
   private static final int NUM_ENTRIES = 200;

   protected WaitNonBlockingStore<Object, Object> store;
   protected ExecutorService executor;
   protected IntSet allSegments;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cb = getDefaultStandaloneCacheConfig(false);
      configurePersistence(cb);
      GlobalConfigurationBuilder global = new GlobalConfigurationBuilder();
      global.globalState().enable().persistentLocation(CommonsTestingUtil.tmpDirectory(this.getClass()));
      global.serialization().addContextInitializer(getSerializationContextInitializer());
      EmbeddedCacheManager manager = TestCacheManagerFactory.createCacheManager(global, cb);
      store = TestingUtil.getFirstStoreWait(manager.getCache());
      executor = testExecutor();
      allSegments = IntSets.immutableRangeSet(manager.getCache().getCacheConfiguration().clustering().hash().numSegments());
      return manager;
   }

   protected abstract void configurePersistence(ConfigurationBuilder cb);

   protected SerializationContextInitializer getSerializationContextInitializer() {
      return TestDataSCI.INSTANCE;
   }

   public void testParallelIterationWithValue() {
      runIterationTest(executor, true);
   }

   public void testSequentialIterationWithValue() {
      runIterationTest(new WithinThreadExecutor(), true);
   }

   public void testParallelIterationWithoutValue() {
      runIterationTest(executor, false);
   }

   public void testSequentialIterationWithoutValue() {
      runIterationTest(new WithinThreadExecutor(), false);
   }

   private void runIterationTest(Executor executor, final boolean fetchValues) {
      final ConcurrentMap<Integer, Integer> entries = new ConcurrentHashMap<>();
      final ConcurrentMap<Integer, Metadata> metadata = new ConcurrentHashMap<>();
      final AtomicBoolean sameKeyMultipleTimes = new AtomicBoolean();

      assertEquals(store.sizeWait(allSegments), 0);
      insertData();

      Flowable<MarshallableEntry<Object, Object>> flowable = Flowable.fromPublisher(
            store.publishEntries(allSegments, null, fetchValues));
      flowable = flowable.doOnNext(me -> {
         Integer key = unwrapKey(me.getKey());
         if (fetchValues) {
            // Note: MarshalledEntryImpl.getValue() fails with NPE when it's got null valueBytes,
            // that's why we must not call this when values are not retrieved
            Integer existing = entries.put(key, unwrapValue(me.getValue()));
            if (existing != null) {
               log.warnf("Already a value present for key %s: %s", key, existing);
               sameKeyMultipleTimes.set(true);
            }
         }
         if (me.getMetadata() != null) {
            log.tracef("For key %d found metadata %s", key, me.getMetadata());
            Metadata prevMetadata = metadata.put(key, me.getMetadata());
            if (prevMetadata != null) {
               log.warnf("Already a metadata present for key %s: %s", key, prevMetadata);
               sameKeyMultipleTimes.set(true);
            }
         } else {
            log.tracef("No metadata found for key %d", key);
         }
      });

      TestSubscriber<MarshallableEntry<Object, Object>> subscriber = TestSubscriber.create(0);
      flowable.subscribe(subscriber);

      int batchsize = 10;
      // Request all elements except the last 10 - do this across different threads
      for (int i = 0; i < NUM_ENTRIES / batchsize - 1; i++) {
         // Now request all the entries on different threads - just to see if the publisher can handle it
         executor.execute(() -> subscriber.request(batchsize));
      }

      // We should receive all of those elements now
      subscriber.awaitCount(NUM_ENTRIES - batchsize);

      // Now request on the main thread - which should guarantee requests from different threads
      // We only have 10 elements left, but request 1 more just because and we can verify we only got 10
      subscriber.request(batchsize + 1);

      subscriber.awaitDone(10, TimeUnit.SECONDS);

      subscriber.assertNoErrors();
      assertEquals(NUM_ENTRIES, subscriber.values().size());

      assertFalse(sameKeyMultipleTimes.get());
      for (int i = 0; i < NUM_ENTRIES; i++) {
         if (fetchValues) {
            assertEquals(entries.get(i), (Integer) i, "For key " + i);
         } else {
            assertNull(entries.get(i), "For key " + i);
         }
         if (hasMetadata(fetchValues, i)) {
            assertNotNull(metadata.get(i), "For key " + i);
            assertEquals(metadata.get(i).lifespan(), lifespan(i), "For key " + i);
            assertEquals(metadata.get(i).maxIdle(), maxIdle(i), "For key " + i);
         } else {
            assertMetadataEmpty(metadata.get(i), i);
         }
      }
   }

   private void insertData() {
      for (int i = 0; i < NUM_ENTRIES; i++) {
         long now = System.currentTimeMillis();
         Metadata metadata = insertMetadata(i) ? TestingUtil.metadata(lifespan(i), maxIdle(i)) : null;
         MarshallableEntry me = MarshalledEntryUtil.create(wrapKey(i), wrapValue(i, i), metadata, now, now, cache);
         store.write(me);
      }
   }

   protected void assertMetadataEmpty(Metadata metadata, Object key) {
      assertNull(metadata, "For key " + key);
   }

   protected boolean insertMetadata(int i) {
      return i % 2 == 0;
   }

   protected boolean hasMetadata(boolean fetchValues, int i) {
      return insertMetadata(i);
   }

   protected long lifespan(int i) {
      return 10000L * (i + 1000);
   }

   protected long maxIdle(int i) {
      return 1000L * (i + 1000);
   }

   protected Object wrapKey(int key) {
      return key;
   }

   protected Integer unwrapKey(Object key) {
      return (Integer) key;
   }

   protected Object wrapValue(int key, int value) {
      return value;
   }

   protected Integer unwrapValue(Object value) {
      return (Integer) value;
   }
}
