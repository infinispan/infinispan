package org.infinispan.persistence;

import static org.infinispan.test.TestingUtil.allEntries;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNotSame;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.ProtoStreamMarshaller;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.commons.marshall.WrappedBytes;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.container.impl.InternalEntryFactory;
import org.infinispan.container.impl.InternalEntryFactoryImpl;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.marshall.TestObjectStreamMarshaller;
import org.infinispan.marshall.persistence.PersistenceMarshaller;
import org.infinispan.marshall.persistence.impl.MarshalledEntryFactoryImpl;
import org.infinispan.marshall.persistence.impl.MarshalledEntryUtil;
import org.infinispan.persistence.spi.CacheLoader;
import org.infinispan.persistence.spi.CacheWriter;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.MarshallableEntryFactory;
import org.infinispan.persistence.spi.NonBlockingStore;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.persistence.support.EnsureNonBlockingStore;
import org.infinispan.persistence.support.NonBlockingStoreAdapter;
import org.infinispan.persistence.support.WaitNonBlockingStore;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.data.Key;
import org.infinispan.test.data.Person;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TestInternalCacheEntryFactory;
import org.infinispan.util.ControlledTimeService;
import org.infinispan.util.PersistenceMockUtil;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import io.reactivex.rxjava3.core.Flowable;

/**
 * This is a base class containing various unit tests for each and every different CacheStore implementations. If you
 * need to add Cache/CacheManager tests that need to be run for each cache store/loader implementation, then use
 * BaseStoreFunctionalTest.
 */
@Test(groups = "unit", testName = "persistence.BaseNonBlockingStoreTest")
public abstract class BaseNonBlockingStoreTest extends AbstractInfinispanTest {

   protected static final int WRITE_DELETE_BATCH_MIN_ENTRIES = 80;
   protected static final int WRITE_DELETE_BATCH_MAX_ENTRIES = 120;
   protected TestObjectStreamMarshaller marshaller;

   protected EnsureNonBlockingStore<Object, Object> store;
   protected ControlledTimeService timeService;
   protected InternalEntryFactory internalEntryFactory;
   protected MarshallableEntryFactory marshallableEntryFactory;
   protected Configuration configuration;
   protected int segmentCount;
   protected InitializationContext initializationContext;
   protected KeyPartitioner keyPartitioner = k -> Math.abs(k.hashCode() % segmentCount);
   protected Set<NonBlockingStore.Characteristic> characteristics;
   private IntSet segments;

   protected static <K, V> NonBlockingStore<K, V> asNonBlockingStore(CacheLoader<K, V> loader) {
      return new NonBlockingStoreAdapter<>(loader);
   }

   protected static <K, V> NonBlockingStore<K, V> asNonBlockingStore(CacheWriter<K, V> writer) {
      return new NonBlockingStoreAdapter<>(writer);
   }

   protected abstract NonBlockingStore createStore() throws Exception;
   protected abstract Configuration buildConfig(ConfigurationBuilder configurationBuilder);

   //alwaysRun = true otherwise, when we run unstable tests, this method is not invoked (because it belongs to the unit group)
   @BeforeMethod(alwaysRun = true)
   public void setUp() throws Exception {
      marshaller = new TestObjectStreamMarshaller(getSerializationContextInitializer());
      timeService = getTimeService();
      internalEntryFactory = new InternalEntryFactoryImpl();
      TestingUtil.inject(internalEntryFactory, timeService);
      marshallableEntryFactory = new MarshalledEntryFactoryImpl();
      TestingUtil.inject(marshallableEntryFactory, marshaller);
      try {
         NonBlockingStore nonBlockingStore = createStore();
         // Make sure all store methods don't block when we invoke them
         store = new EnsureNonBlockingStore<Object, Object>() {
            @Override
            public NonBlockingStore<Object, Object> delegate() {
               return nonBlockingStore;
            }

            @Override
            public KeyPartitioner getKeyPartitioner() {
               return keyPartitioner;
            }
         };

         startStore(store);
      } catch (Exception e) {
         log.error("Error creating store", e);
         throw e;
      }
   }

   private void startStore(WaitNonBlockingStore<?, ?> store) {
      // Reuse the same configuration between restarts
      if (configuration == null) {
         ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
         setConfiguration(buildConfig(builder));
      }
      store.startAndWait(createContext(configuration));
      characteristics = store.characteristics();
   }

   protected Object keyToStorage(Object key) {
      return key;
   }

   protected Object valueToStorage(Object value) {
      return value;
   }

   protected void setConfiguration(Configuration configuration) {
      this.configuration = configuration;
      this.segmentCount = configuration.clustering().hash().numSegments();
      segments = IntSets.immutableRangeSet(segmentCount);
   }

   //alwaysRun = true otherwise, when we run unstable tests, this method is not invoked (because it belongs to the unit group)
   @AfterMethod(alwaysRun = true)
   public void tearDown() throws PersistenceException {
      try {
         if (store != null) {
            store.clearAndWait();
            store.stopAndWait();
         }
         if (marshaller != null) {
            marshaller.stop();
         }
      } finally {
         store = null;
      }
   }

   /**
    * @return a mock marshaller for use with the cache store impls
    */
   protected PersistenceMarshaller getMarshaller() {
      return marshaller;
   }

   /**
    * @return the {@link SerializationContextInitializer} used to initiate the user marshaller
    */
   protected SerializationContextInitializer getSerializationContextInitializer() {
      return TestDataSCI.INSTANCE;
   }

   /**
    * To be overridden if the store requires special time handling
    */
   protected ControlledTimeService getTimeService() {
      return new ControlledTimeService();
   }

   /**
    * Overridden in stores which accept only certain value types
    */
   protected Object wrap(Object key, Object value) {
      return value;
   }

   public void testLoadAndStoreImmortal() throws PersistenceException {
      assertIsEmpty();
      store.write(marshalledEntry("k", "v"));

      MarshallableEntry entry = store.loadEntry(keyToStorage("k"));
      assertEquals(valueToStorage("v"), entry.getValue());
      assertTrue("Expected an immortalEntry",
                 entry.getMetadata() == null || entry.expiryTime() == -1 || entry.getMetadata().maxIdle() == -1);
      assertContains("k", true);
      // The store may return null or FALSE but not TRUE
      assertNotSame(Boolean.TRUE, store.delete(keyToStorage("k2")));
   }

   public void testLoadAndStoreWithLifespan() throws Exception {
      // Store doesn't support expiration, so don't test it
      if (!characteristics.contains(NonBlockingStore.Characteristic.EXPIRATION)) {
         return;
      }
      assertIsEmpty();

      long lifespan = 120000;
      InternalCacheEntry se = internalCacheEntry("k", "v", lifespan);
      assertExpired(se, false);
      store.write(marshalledEntry(se));

      assertContains("k", true);
      assertCorrectExpiry(store.loadEntry(keyToStorage("k")), "v", lifespan, -1, false);
      assertCorrectExpiry(TestingUtil.allEntries(store, segments).iterator().next(), "v", lifespan, -1, false);
      timeService.advance(lifespan + 1);

      lifespan = 2000;
      se = internalCacheEntry("k", "v", lifespan);
      assertExpired(se, false);
      store.write(marshalledEntry(se));
      timeService.advance(lifespan + 1);
      purgeExpired("k");
      assertExpired(se, true);
      assertEventuallyExpires("k");
      assertContains("k", false);
      assertIsEmpty();
   }

   private void assertCorrectExpiry(MarshallableEntry me, String value, long lifespan, long maxIdle, boolean expired) {
      assertNotNull(String.valueOf(me), me);
      assertEquals(me + ".getValue()", valueToStorage(value), me.getValue());

      if (lifespan > -1) {
         assertNotNull(me + ".getMetadata()", me.getMetadata());
         assertEquals(me + ".getMetadata().lifespan()", lifespan, me.getMetadata().lifespan());
         assertTrue(me + ".created() > -1", me.created() > -1);
      }
      if (maxIdle > -1) {
         assertNotNull(me + ".getMetadata()", me.getMetadata());
         assertEquals(me + ".getMetadata().maxIdle()", maxIdle, me.getMetadata().maxIdle());
         assertTrue(me + ".lastUsed() > -1", me.lastUsed() > -1);
      }
      if (me.getMetadata() != null) {
         assertEquals(me + ".isExpired() ", expired, me.isExpired(timeService.wallClockTime()));
      }
   }


   public void testLoadAndStoreWithIdle() throws Exception {
      // Store doesn't support expiration, so don't test it
      if (!characteristics.contains(NonBlockingStore.Characteristic.EXPIRATION)) {
         return;
      }
      assertIsEmpty();

      long idle = 120000;
      InternalCacheEntry se = internalCacheEntry("k", "v", -1, idle);
      assertExpired(se, false);
      store.write(marshalledEntry(se));

      assertContains("k", true);
      assertCorrectExpiry(store.loadEntry(keyToStorage("k")), "v", -1, idle, false);
      assertCorrectExpiry(TestingUtil.allEntries(store, segments).iterator().next(), "v", -1, idle, false);
      timeService.advance(idle + 1);

      idle = 1000;
      se = internalCacheEntry("k", "v", -1, idle);
      assertExpired(se, false);
      store.write(marshalledEntry(se));
      timeService.advance(idle + 1);
      purgeExpired("k");
      assertExpired(se, true);
      assertEventuallyExpires("k");
      assertContains("k", false);
      assertIsEmpty();
   }

   private void assertIsEmpty() {
      assertEmpty(TestingUtil.allEntries(store, segments), true);
   }

   protected void assertEventuallyExpires(final String key) throws Exception {
      Object storageKey = keyToStorage(key);
      eventually(() -> store.loadEntry(storageKey) == null);
   }

   /* Override if the store cannot purge all expired entries upon request */
   protected boolean storePurgesAllExpired() {
      return true;
   }

   protected void purgeExpired(String... expiredKeys) {
      final Set<Object> expired = Stream.of(expiredKeys).map(this::keyToStorage).collect(Collectors.toSet());
      List<MarshallableEntry<Object, Object>> expiredList = store.purge();

      expiredList.removeIf(me -> expired.remove(me.getKey()));

      assertEmpty(expiredList, true);
      if (storePurgesAllExpired()) {
         assertEmpty(expired, true);
      }
      assertEquals(Collections.emptyList(), expiredList);
   }

   public void testLoadAndStoreWithLifespanAndIdle() throws Exception {
      // Store doesn't support expiration, so don't test it
      if (!characteristics.contains(NonBlockingStore.Characteristic.EXPIRATION)) {
         return;
      }
      assertIsEmpty();

      long lifespan = 200000;
      long idle = 120000;
      InternalCacheEntry se = internalCacheEntry("k", "v", lifespan, idle);
      InternalCacheValue icv = se.toInternalCacheValue();
      assertEquals(se.getCreated(), icv.getCreated());
      assertEquals(se.getLastUsed(), icv.getLastUsed());
      assertExpired(se, false);
      store.write(marshalledEntry(se));

      assertContains("k", true);
      assertCorrectExpiry(store.loadEntry(keyToStorage("k")), "v", lifespan, idle, false);
      assertCorrectExpiry(TestingUtil.allEntries(store, segments).iterator().next(), "v", lifespan, idle, false);
      timeService.advance(idle + 1);

      idle = 1000;
      lifespan = 4000;
      se = internalCacheEntry("k", "v", lifespan, idle);
      assertExpired(se, false);
      store.write(marshalledEntry(se));
      timeService.advance(idle + 1);
      purgeExpired("k");
      assertExpired(se, true); //expired by idle
      assertEventuallyExpires("k");
      assertContains("k", false);
      assertIsEmpty();
   }

   public void testLoadAndStoreWithLifespanAndIdle2() throws Exception {
      // Store doesn't support expiration, so don't test it
      if (!characteristics.contains(NonBlockingStore.Characteristic.EXPIRATION)) {
         return;
      }
      assertContains("k", false);

      long lifespan = 2000;
      long idle = 2000;
      InternalCacheEntry se = internalCacheEntry("k", "v", lifespan, idle);
      InternalCacheValue icv = se.toInternalCacheValue();
      assertEquals(se.getCreated(), icv.getCreated());
      assertEquals(se.getLastUsed(), icv.getLastUsed());
      assertExpired(se, false);
      store.write(marshalledEntry(se));

      assertContains("k", true);
      assertCorrectExpiry(store.loadEntry(keyToStorage("k")), "v", lifespan, idle, false);
      assertCorrectExpiry(TestingUtil.allEntries(store, segments).iterator().next(), "v", lifespan, idle, false);

      idle = 4000;
      lifespan = 2000;
      se = internalCacheEntry("k", "v", lifespan, idle);
      assertExpired(se, false);
      store.write(marshalledEntry(se));

      timeService.advance(lifespan + 1);
      assertExpired(se, true); //expired by lifespan

      purgeExpired("k");

      assertEventuallyExpires("k");
      assertContains("k", false);

      assertIsEmpty();
   }

   public void testStopStartDoesNotNukeValues() throws InterruptedException, PersistenceException {
      assertIsEmpty();

      long lifespan = 1000;
      long idle = 1000;
      InternalCacheEntry se1 = internalCacheEntry("k1", "v1", lifespan);
      InternalCacheEntry se2 = internalCacheEntry("k2", "v2", -1);
      InternalCacheEntry se3 = internalCacheEntry("k3", "v3", -1, idle);
      InternalCacheEntry se4 = internalCacheEntry("k4", "v4", lifespan, idle);

      assertExpired(se1, false);
      assertExpired(se2, false);
      assertExpired(se3, false);
      assertExpired(se4, false);

      store.write(marshalledEntry(se1));
      store.write(marshalledEntry(se2));
      store.write(marshalledEntry(se3));
      store.write(marshalledEntry(se4));

      timeService.advance(lifespan + 1);
      assertExpired(se1, true);
      assertExpired(se2, false);
      assertExpired(se3, true);
      assertExpired(se4, true);

      store.stopAndWait();

      startStore(store);
      assertExpired(se1, true);
      assertNull(store.loadEntry(keyToStorage("k1")));
      assertContains("k1", false);
      assertExpired(se2, false);
      assertNotNull(store.loadEntry(keyToStorage("k2")));
      assertContains("k2", true);
      assertEquals(valueToStorage("v2"), store.loadEntry(keyToStorage("k2")).getValue());
      assertExpired(se3, true);
      assertNull(store.loadEntry(keyToStorage("k3")));
      assertContains("k3", false);
      assertExpired(se4, true);
      assertNull(store.loadEntry(keyToStorage("k4")));
      assertContains("k4", false);
   }

   public void testPreload() throws Exception {
      assertIsEmpty();

      store.write(marshalledEntry("k1", "v1"));
      store.write(marshalledEntry("k2", "v2"));
      store.write(marshalledEntry("k3", "v3"));

      Set<MarshallableEntry<Object, Object>> set = TestingUtil.allEntries(store, segments);

      assertSize(set, 3);
      Set<Object> expected = Stream.of("k1", "k2", "k3")
            .map(this::keyToStorage)
            .collect(Collectors.toSet());
      for (MarshallableEntry se : set) {
         assertTrue(expected.remove(se.getKey()));
      }

      assertEmpty(expected, true);
   }

   public void testStoreAndRemove() throws PersistenceException {
      assertIsEmpty();

      store.write(marshalledEntry("k1", "v1"));
      store.write(marshalledEntry("k2", "v2"));
      store.write(marshalledEntry("k3", "v3"));
      store.write(marshalledEntry("k4", "v4"));


      Set<MarshallableEntry<Object, Object>> set = TestingUtil.allEntries(store, segments);

      assertSize(set, 4);

      Set<Object> expected = Stream.of("k1", "k2", "k3", "k4")
            .map(this::keyToStorage)
            .collect(Collectors.toSet());

      for (MarshallableEntry se : set) {
         assertTrue(expected.remove(se.getKey()));
      }

      assertEmpty(expected, true);

      store.delete(keyToStorage("k1"));
      store.delete(keyToStorage("k2"));
      store.delete(keyToStorage("k3"));

      set = TestingUtil.allEntries(store, segments);
      assertSize(set, 1);
      assertEquals(keyToStorage("k4"), set.iterator().next().getKey());

      assertEquals(1, store.publishKeysWait(segments, null).size());
   }

   public void testSizeApproximation() {
      assertIsEmpty();

      store.write(marshalledEntry("k1", "v1"));
      store.write(marshalledEntry("k2", "v2"));
      store.write(marshalledEntry("k3", "v3"));
      store.write(marshalledEntry("k4", "v4"));

      assertEquals(4, store.approximateSizeWait(segments));
   }

   public void testPurgeExpired() throws Exception {
      // Store doesn't support expiration, so don't test it
      if (!characteristics.contains(NonBlockingStore.Characteristic.EXPIRATION)) {
         return;
      }
      assertIsEmpty();
      // Increased lifespan and idle timeouts to accommodate slower cache stores
      // checking if cache store contains the entry right after inserting because
      // some slower cache stores (seen on DB2) don't manage to entry all the entries
      // before running out of lifespan making this test unpredictably fail on them.

      long lifespan = 7000;
      long idle = 2000;

      InternalCacheEntry ice1 = internalCacheEntry("k1", "v1", lifespan);
      store.write(marshalledEntry(ice1));
      assertContains("k1", true);

      InternalCacheEntry ice2 = internalCacheEntry("k2", "v2", -1, idle);
      store.write(marshalledEntry(ice2));
      assertContains("k2", true);

      InternalCacheEntry ice3 = internalCacheEntry("k3", "v3", lifespan, idle);
      store.write(marshalledEntry(ice3));
      assertContains("k3", true);

      InternalCacheEntry ice4 = internalCacheEntry("k4", "v4", -1, -1);
      store.write(marshalledEntry(ice4)); // immortal entry
      assertContains("k4", true);

      InternalCacheEntry ice5 = internalCacheEntry("k5", "v5", lifespan * 1000, idle * 1000);
      store.write(marshalledEntry(ice5)); // long life mortal entry
      assertContains("k5", true);


      timeService.advance(lifespan + 1);

      // Make sure we don't report that we contain these values
      assertContains("k1", false);
      assertContains("k2", false);
      assertContains("k3", false);
      assertContains("k4", true);
      assertContains("k5", true);

      purgeExpired("k1", "k2", "k3");

      assertContains("k1", false);
      assertContains("k2", false);
      assertContains("k3", false);
      assertContains("k4", true);
      assertContains("k5", true);
   }

   public void testLoadAll() throws PersistenceException {
      assertIsEmpty();

      store.write(marshalledEntry("k1", "v1"));
      store.write(marshalledEntry("k2", "v2"));
      store.write(marshalledEntry("k3", "v3"));
      store.write(marshalledEntry("k4", "v4"));
      store.write(marshalledEntry("k5", "v5"));

      Set<MarshallableEntry<Object, Object>> s = allEntries(store, segments);
      assertSize(s, 5);

      s = allEntries(store, segments, k -> true);
      assertSize(s, 5);

      Object storedK3 = keyToStorage("k3");

      s = allEntries(store, segments, k -> !storedK3.equals(k));
      assertSize(s, 4);

      for (MarshallableEntry me : s) {
         assertFalse(me.getKey().equals(storedK3));
      }
   }

   public void testReplaceEntry() {
      assertIsEmpty();
      InternalCacheEntry ice = internalCacheEntry("k1", "v1", -1);
      store.write(marshalledEntry(ice));
      assertEquals(valueToStorage("v1"), store.loadEntry(keyToStorage("k1")).getValue());

      InternalCacheEntry ice2 = internalCacheEntry("k1", "v2", -1);
      store.write(marshalledEntry(ice2));

      assertEquals(valueToStorage("v2"), store.loadEntry(keyToStorage("k1")).getValue());
   }

   public void testReplaceExpiredEntry() throws Exception {
      assertIsEmpty();
      final long lifespan = 3000;
      InternalCacheEntry ice = internalCacheEntry("k1", "v1", lifespan);
      assertExpired(ice, false);
      store.write(marshalledEntry(ice));
      Object storedKey = keyToStorage("k1");
      assertEquals("v1", store.loadEntry(storedKey).getValue());

      timeService.advance(lifespan + 1);
      assertExpired(ice, true);

      assertNull(store.loadEntry(storedKey));

      InternalCacheEntry ice2 = internalCacheEntry("k1", "v2", lifespan);
      assertExpired(ice2, false);
      store.write(marshalledEntry(ice2));

      assertEquals(valueToStorage("v2"), store.loadEntry(storedKey).getValue());

      timeService.advance(lifespan + 1);
      assertExpired(ice2, true);

      assertNull(store.loadEntry(storedKey));
   }

   public void testLoadAndStoreBytesValues() throws PersistenceException, IOException, InterruptedException {
      assertIsEmpty();

      SerializationContext ctx = ProtobufUtil.newSerializationContext();
      SerializationContextInitializer sci = TestDataSCI.INSTANCE;
      sci.registerSchema(ctx);
      sci.registerMarshallers(ctx);
      Marshaller userMarshaller = new ProtoStreamMarshaller(ctx);
      WrappedBytes key = new WrappedByteArray(userMarshaller.objectToByteBuffer(new Key("key")));
      WrappedBytes key2 = new WrappedByteArray(userMarshaller.objectToByteBuffer(new Key("key2")));
      WrappedBytes value = new WrappedByteArray(userMarshaller.objectToByteBuffer(new Person()));

      assertFalse(store.contains(key));
      PersistenceMarshaller persistenceMarshaller = getMarshaller();
      store.write(MarshalledEntryUtil.create(key, value, persistenceMarshaller));

      assertEquals(value, store.loadEntry(key).getValue());
      MarshallableEntry entry = store.loadEntry(key);
      assertTrue("Expected an immortalEntry",
                 entry.getMetadata() == null || entry.expiryTime() == -1 || entry.getMetadata().maxIdle() == -1);
      assertTrue(store.contains(key));

      // Delete return value is optional
      // The store may return null or FALSE but not TRUE
      assertNotSame(Boolean.TRUE, store.delete(key2));
      // The store may return null or TRUE but not FALSE
      assertNotSame(Boolean.FALSE, store.delete(key));
   }

   public void testWriteAndDeleteBatch() {
      // Number of entries is randomized to even numbers between 80 and 120
      int numberOfEntries = 2 * ThreadLocalRandom.current().nextInt(WRITE_DELETE_BATCH_MIN_ENTRIES / 2, WRITE_DELETE_BATCH_MAX_ENTRIES / 2 + 1);
      testBatch(numberOfEntries, () -> store.batchUpdate(segmentCount, Flowable.empty(),
            TestingUtil.multipleSegmentPublisher(Flowable.range(0, numberOfEntries).map(i -> marshalledEntry(i.toString(), "Val" + i)),
            MarshallableEntry::getKey, keyPartitioner)));
   }

   public void testWriteAndDeleteBatchIterable() {
      // Number of entries is randomized to even numbers between 80 and 120
      int numberOfEntries = 2 * ThreadLocalRandom.current().nextInt(WRITE_DELETE_BATCH_MIN_ENTRIES / 2, WRITE_DELETE_BATCH_MAX_ENTRIES / 2 + 1);
      testBatch(numberOfEntries, () -> store.batchUpdate(segmentCount, Flowable.empty(),
            TestingUtil.multipleSegmentPublisher(Flowable.range(0, numberOfEntries).map(i -> marshalledEntry(i.toString(), "Val" + i)),
            MarshallableEntry::getKey, keyPartitioner)));
   }

   public void testEmptyWriteAndDeleteBatchIterable() {
      assertIsEmpty();
      assertNull("should not be present in the store", store.loadEntry(keyToStorage(0)));
      store.batchUpdate(1, Flowable.empty(), Flowable.empty());

      assertEquals(0, store.sizeWait(segments));
   }

   private void testBatch(int numberOfEntries, Runnable createBatch) {
      assertIsEmpty();
      assertNull("should not be present in the store", store.loadEntry(keyToStorage(0)));

      createBatch.run();

      Set<MarshallableEntry<Object, Object>> set = TestingUtil.allEntries(store, segments);
      assertSize(set, numberOfEntries);
      assertNotNull(store.loadEntry(keyToStorage("56")));

      int batchSize = numberOfEntries / 2;
      List<Object> keys = IntStream.range(0, batchSize).mapToObj(Integer::toString).map(this::keyToStorage).collect(Collectors.toList());
      store.batchUpdate(segmentCount, TestingUtil.multipleSegmentPublisher(Flowable.fromIterable(keys),
            Function.identity(), keyPartitioner), Flowable.empty());
      set = TestingUtil.allEntries(store, segments);
      assertSize(set, batchSize);
      assertNull(store.loadEntry(keyToStorage("20")));
   }

   public void testIsAvailable() {
      assertTrue(store.checkAvailable());
   }

   protected final InitializationContext createContext(Configuration configuration) {
      PersistenceMockUtil.InvocationContextBuilder builder = new PersistenceMockUtil.InvocationContextBuilder(getClass(), configuration, getMarshaller())
            .setTimeService(timeService)
            .setKeyPartitioner(keyPartitioner);
      modifyInitializationContext(builder);
      initializationContext = builder.build();
      return initializationContext;
   }

   protected void modifyInitializationContext(PersistenceMockUtil.InvocationContextBuilder contextBuilder) {
      // Default does nothing
   }

   protected final void assertContains(Object k, boolean expected) {
      Object transformedKey = keyToStorage(k);
      assertEquals("contains(" + transformedKey + ")", expected, store.contains(transformedKey));
   }

   protected final <K> InternalCacheEntry<K, Object> internalCacheEntry(K key, Object value, long lifespan) {
      Object transformedKey = keyToStorage(key);
      Object transformedValue = valueToStorage(value);
      return TestInternalCacheEntryFactory.create(internalEntryFactory, (K) transformedKey, wrap(transformedKey, transformedValue), lifespan);
   }

   private InternalCacheEntry<Object, Object> internalCacheEntry(String key, String value, long lifespan, long idle) {
      Object transformedKey = keyToStorage(key);
      Object transformedValue = valueToStorage(value);
      return TestInternalCacheEntryFactory.create(internalEntryFactory, transformedKey, wrap(transformedKey, transformedValue), lifespan, idle);
   }

   private MarshallableEntry<Object, Object> marshalledEntry(String key, String value) {
      Object transformedKey = keyToStorage(key);
      Object transformedValue = valueToStorage(value);
      return MarshalledEntryUtil.create(transformedKey, wrap(transformedKey, transformedValue), getMarshaller());
   }

   protected final MarshallableEntry<Object, Object> marshalledEntry(InternalCacheEntry entry) {
      //noinspection unchecked
      return MarshalledEntryUtil.create(entry, getMarshaller());
   }

   private void assertSize(Collection<?> collection, int expected) {
      assertEquals(collection + ".size()", expected, collection.size());
   }

   private void assertExpired(InternalCacheEntry entry, boolean expected) {
      assertEquals(entry + ".isExpired() ", expected, entry.isExpired(timeService.wallClockTime()));
   }

   private void assertEmpty(Collection<?> collection, boolean expected) {
      assertEquals(collection + ".isEmpty()", expected, collection.isEmpty());
   }
}
