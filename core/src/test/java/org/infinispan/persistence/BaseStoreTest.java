package org.infinispan.persistence;

import static org.infinispan.test.TestingUtil.allEntries;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.commons.marshall.WrappedBytes;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.impl.InternalEntryFactory;
import org.infinispan.container.impl.InternalEntryFactoryImpl;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.marshall.TestObjectStreamMarshaller;
import org.infinispan.marshall.core.ExternalPojo;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.marshall.core.MarshalledEntryImpl;
import org.infinispan.metadata.InternalMetadata;
import org.infinispan.persistence.spi.AdvancedCacheExpirationWriter;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestInternalCacheEntryFactory;
import org.infinispan.util.ControlledTimeService;
import org.infinispan.util.PersistenceMockUtil;
import org.infinispan.util.concurrent.WithinThreadExecutor;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import io.reactivex.Flowable;

/**
 * This is a base class containing various unit tests for each and every different CacheStore implementations. If you
 * need to add Cache/CacheManager tests that need to be run for each cache store/loader implementation, then use
 * BaseStoreFunctionalTest.
 */
// this needs to be here for the test to run in an IDE
@Test(groups = "unit", testName = "persistence.BaseStoreTest")
public abstract class BaseStoreTest extends AbstractInfinispanTest {

   protected static final int WRITE_DELETE_BATCH_MIN_ENTRIES = 80;
   protected static final int WRITE_DELETE_BATCH_MAX_ENTRIES = 120;
   private TestObjectStreamMarshaller marshaller;
   protected abstract AdvancedLoadWriteStore createStore() throws Exception;

   protected AdvancedLoadWriteStore<Object, Object> cl;
   protected ControlledTimeService timeService;
   private InternalEntryFactory factory;

   //alwaysRun = true otherwise, when we run unstable tests, this method is not invoked (because it belongs to the unit group)
   @BeforeMethod(alwaysRun = true)
   public void setUp() throws Exception {
      marshaller = new TestObjectStreamMarshaller();
      timeService = getTimeService();
      factory = new InternalEntryFactoryImpl();
      TestingUtil.inject(factory, timeService);
      try {
         //noinspection unchecked
         cl = createStore();
         cl.start();
      } catch (Exception e) {
         log.error("Error creating store", e);
         throw e;
      }
   }

   //alwaysRun = true otherwise, when we run unstable tests, this method is not invoked (because it belongs to the unit group)
   @AfterMethod(alwaysRun = true)
   public void tearDown() throws PersistenceException {
      try {
         if (cl != null) {
            cl.clear();
            cl.stop();
         }
         if (marshaller != null) {
            marshaller.stop();
         }
      } finally {
         cl = null;
      }
   }

   /**
    * @return a mock marshaller for use with the cache store impls
    */
   protected StreamingMarshaller getMarshaller() {
      return marshaller;
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
   protected Object wrap(String key, String value) {
      return value;
   }

   /**
    * Overridden in stores which accept only certain value types
    */
   protected String unwrap(Object wrapped) {
      return (String) wrapped;
   }

   public void testLoadAndStoreImmortal() throws PersistenceException {
      assertIsEmpty();
      cl.write(marshalledEntry("k", "v", null));

      MarshalledEntry entry = cl.load("k");
      assertEquals("v", unwrap(entry.getValue()));
      assertTrue("Expected an immortalEntry",
                 entry.getMetadata() == null || entry.getMetadata().expiryTime() == -1 || entry.getMetadata().maxIdle() == -1);
      assertContains("k", true);
      assertFalse(cl.delete("k2"));
   }

   public void testLoadAndStoreWithLifespan() throws Exception {
      assertIsEmpty();

      long lifespan = 120000;
      InternalCacheEntry se = internalCacheEntry("k", "v", lifespan);
      assertExpired(se, false);
      cl.write(marshalledEntry(se));

      assertContains("k", true);
      assertCorrectExpiry(cl.load("k"), "v", lifespan, -1, false);
      assertCorrectExpiry(TestingUtil.allEntries(cl).iterator().next(), "v", lifespan, -1, false);

      lifespan = 2000;
      se = internalCacheEntry("k", "v", lifespan);
      assertExpired(se, false);
      cl.write(marshalledEntry(se));
      timeService.advance(lifespan + 1);
      purgeExpired("k");
      assertExpired(se, true);
      assertEventuallyExpires("k");
      assertContains("k", false);
      assertIsEmpty();
   }

   private void assertCorrectExpiry(MarshalledEntry me, String value, long lifespan, long maxIdle, boolean expired) {
      assertNotNull(String.valueOf(me), me);
      assertEquals(me + ".getValue()", value, unwrap(me.getValue()));

      if (lifespan > -1) {
         assertNotNull(me + ".getMetadata()", me.getMetadata());
         assertEquals(me + ".getMetadata().lifespan()", lifespan, me.getMetadata().lifespan());
         assertTrue(me + ".getMetadata().created() > -1", me.getMetadata().created() > -1);
      }
      if (maxIdle > -1) {
         assertNotNull(me + ".getMetadata()", me.getMetadata());
         assertEquals(me + ".getMetadata().maxIdle()", maxIdle, me.getMetadata().maxIdle());
         assertTrue(me + ".getMetadata().lastUsed() > -1", me.getMetadata().lastUsed() > -1);
      }
      if (me.getMetadata() != null) {
         assertEquals(me + "getMetadata().isExpired() ", expired, me.getMetadata().isExpired(timeService.wallClockTime()));
      }
   }


   public void testLoadAndStoreWithIdle() throws Exception {
      assertIsEmpty();

      long idle = 120000;
      InternalCacheEntry se = internalCacheEntry("k", "v", -1, idle);
      assertExpired(se, false);
      cl.write(marshalledEntry(se));

      assertContains("k", true);
      assertCorrectExpiry(cl.load("k"), "v", -1, idle, false);
      assertCorrectExpiry(TestingUtil.allEntries(cl).iterator().next(), "v", -1, idle, false);

      idle = 1000;
      se = internalCacheEntry("k", "v", -1, idle);
      assertExpired(se, false);
      cl.write(marshalledEntry(se));
      timeService.advance(idle + 1);
      purgeExpired("k");
      assertExpired(se, true);
      assertEventuallyExpires("k");
      assertContains("k", false);
      assertIsEmpty();
   }

   private void assertIsEmpty() {
      assertEmpty(TestingUtil.allEntries(cl), true);
   }

   protected void assertEventuallyExpires(final String key) throws Exception {
      eventually(() -> cl.load(key) == null);
   }

   /* Override if the store cannot purge all expired entries upon request */
   protected boolean storePurgesAllExpired() {
      return true;
   }

   protected void purgeExpired(String... expiredKeys) throws Exception {
      final Set<String> expired = new HashSet<>(Arrays.asList(expiredKeys));
      final Set<Object> incorrect = new HashSet<>();
      if (cl instanceof AdvancedCacheExpirationWriter) {
         final AdvancedCacheExpirationWriter.ExpirationPurgeListener purgeListener = new AdvancedCacheExpirationWriter.ExpirationPurgeListener() {
            @Override
            public void marshalledEntryPurged(MarshalledEntry entry) {
               Object key = entry.getKey();
               if (!expired.remove(key)) {
                  incorrect.add(key);
               }
            }

            @Override
            public void entryPurged(Object key) {
               if (!expired.remove(key)) {
                  incorrect.add(key);
               }
            }
         };
         ((AdvancedCacheExpirationWriter) cl).purge(new WithinThreadExecutor(), purgeListener);
      } else {
         //noinspection unchecked
         cl.purge(new WithinThreadExecutor(), key -> {
            if (!expired.remove(key))
               incorrect.add(key);
         });
      }

      assertEmpty(incorrect, true);
      assertTrue(expired.isEmpty() || !storePurgesAllExpired());
      assertEquals(Collections.emptySet(), incorrect);
   }

   public void testLoadAndStoreWithLifespanAndIdle() throws Exception {
      assertIsEmpty();

      long lifespan = 200000;
      long idle = 120000;
      InternalCacheEntry se = internalCacheEntry("k", "v", lifespan, idle);
      InternalCacheValue icv = se.toInternalCacheValue();
      assertEquals(se.getCreated(), icv.getCreated());
      assertEquals(se.getLastUsed(), icv.getLastUsed());
      assertExpired(se, false);
      cl.write(marshalledEntry(se));

      assertContains("k", true);
      assertCorrectExpiry(cl.load("k"), "v", lifespan, idle, false);
      assertCorrectExpiry(TestingUtil.allEntries(cl).iterator().next(), "v", lifespan, idle, false);

      idle = 1000;
      lifespan = 4000;
      se = internalCacheEntry("k", "v", lifespan, idle);
      assertExpired(se, false);
      cl.write(marshalledEntry(se));
      timeService.advance(idle + 1);
      purgeExpired("k");
      assertExpired(se, true); //expired by idle
      assertEventuallyExpires("k");
      assertContains("k", false);
      assertIsEmpty();
   }

   public void testLoadAndStoreWithLifespanAndIdle2() throws Exception {
      assertContains("k", false);

      long lifespan = 2000;
      long idle = 2000;
      InternalCacheEntry se = internalCacheEntry("k", "v", lifespan, idle);
      InternalCacheValue icv = se.toInternalCacheValue();
      assertEquals(se.getCreated(), icv.getCreated());
      assertEquals(se.getLastUsed(), icv.getLastUsed());
      assertExpired(se, false);
      cl.write(marshalledEntry(se));

      assertContains("k", true);
      assertCorrectExpiry(cl.load("k"), "v", lifespan, idle, false);
      assertCorrectExpiry(TestingUtil.allEntries(cl).iterator().next(), "v", lifespan, idle, false);

      idle = 4000;
      lifespan = 2000;
      se = internalCacheEntry("k", "v", lifespan, idle);
      assertExpired(se, false);
      cl.write(marshalledEntry(se));

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

      cl.write(marshalledEntry(se1));
      cl.write(marshalledEntry(se2));
      cl.write(marshalledEntry(se3));
      cl.write(marshalledEntry(se4));

      timeService.advance(lifespan + 1);
      assertExpired(se1, true);
      assertExpired(se2, false);
      assertExpired(se3, true);
      assertExpired(se4, true);

      cl.stop();
      cl.start();
      assertExpired(se1, true);
      assertNull(cl.load("k1"));
      assertContains("k1", false);
      assertExpired(se2, false);
      assertNotNull(cl.load("k2"));
      assertContains("k2", true);
      assertEquals("v2", unwrap(cl.load("k2").getValue()));
      assertExpired(se3, true);
      assertNull(cl.load("k3"));
      assertContains("k3", false);
      assertExpired(se4, true);
      assertNull(cl.load("k4"));
      assertContains("k4", false);
   }

   public void testPreload() throws Exception {
      assertIsEmpty();

      cl.write(marshalledEntry("k1", "v1", null));
      cl.write(marshalledEntry("k2", "v2", null));
      cl.write(marshalledEntry("k3", "v3", null));

      Set<MarshalledEntry<Object, Object>> set = TestingUtil.allEntries(cl);

      assertSize(set, 3);
      Set<String> expected = new HashSet<>(Arrays.asList("k1", "k2", "k3"));
      for (MarshalledEntry se : set) {
         assertTrue(expected.remove(se.getKey()));
      }

      assertEmpty(expected, true);
   }

   public void testStoreAndRemove() throws PersistenceException {
      assertIsEmpty();

      cl.write(marshalledEntry("k1", "v1", null));
      cl.write(marshalledEntry("k2", "v2", null));
      cl.write(marshalledEntry("k3", "v3", null));
      cl.write(marshalledEntry("k4", "v4", null));


      Set<MarshalledEntry<Object, Object>> set = TestingUtil.allEntries(cl);

      assertSize(set, 4);

      Set<String> expected = new HashSet<>(Arrays.asList("k1", "k2", "k3", "k4"));

      for (MarshalledEntry se : set) {
         assertTrue(expected.remove(se.getKey()));
      }

      assertEmpty(expected, true);

      cl.delete("k1");
      cl.delete("k2");
      cl.delete("k3");

      set = TestingUtil.allEntries(cl);
      assertSize(set, 1);
      assertEquals("k4", set.iterator().next().getKey());

      assertEquals(1, PersistenceUtil.toKeySet(cl, null).size());
      assertEquals(1, Flowable.fromPublisher(cl.publishKeys(null)).count().blockingGet().intValue());
   }

   public void testPurgeExpired() throws Exception {
      assertIsEmpty();
      // Increased lifespan and idle timeouts to accommodate slower cache stores
      // checking if cache store contains the entry right after inserting because
      // some slower cache stores (seen on DB2) don't manage to entry all the entries
      // before running out of lifespan making this test unpredictably fail on them.

      long lifespan = 7000;
      long idle = 5000;

      InternalCacheEntry ice1 = internalCacheEntry("k1", "v1", lifespan);
      cl.write(marshalledEntry(ice1));
      assertContains("k1", true);

      InternalCacheEntry ice2 = internalCacheEntry("k2", "v2", -1, idle);
      cl.write(marshalledEntry(ice2));
      assertContains("k2", true);

      InternalCacheEntry ice3 = internalCacheEntry("k3", "v3", lifespan, idle);
      cl.write(marshalledEntry(ice3));
      assertContains("k3", true);

      InternalCacheEntry ice4 = internalCacheEntry("k4", "v4", -1, -1);
      cl.write(marshalledEntry(ice4)); // immortal entry
      assertContains("k4", true);

      InternalCacheEntry ice5 = internalCacheEntry("k5", "v5", lifespan * 1000, idle * 1000);
      cl.write(marshalledEntry(ice5)); // long life mortal entry
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

      cl.write(marshalledEntry("k1", "v1", null));
      cl.write(marshalledEntry("k2", "v2", null));
      cl.write(marshalledEntry("k3", "v3", null));
      cl.write(marshalledEntry("k4", "v4", null));
      cl.write(marshalledEntry("k5", "v5", null));

      Set<MarshalledEntry<Object, Object>> s = TestingUtil.allEntries(cl);
      assertSize(s, 5);

      s = allEntries(cl, k -> true);
      assertSize(s, 5);

      s = allEntries(cl, k -> !"k3".equals(k));
      assertSize(s, 4);

      for (MarshalledEntry me : s) {
         assertFalse(me.getKey().equals("k3"));
      }
   }

   public void testReplaceExpiredEntry() throws Exception {
      assertIsEmpty();
      final long lifespan = 3000;
      InternalCacheEntry ice = internalCacheEntry("k1", "v1", lifespan);
      assertExpired(ice, false);
      cl.write(marshalledEntry(ice));
      assertEquals("v1", unwrap(cl.load("k1").getValue()));

      timeService.advance(lifespan + 1);
      assertExpired(ice, true);

      assertNull(cl.load("k1"));

      InternalCacheEntry ice2 = internalCacheEntry("k1", "v2", lifespan);
      assertExpired(ice2, false);
      cl.write(marshalledEntry(ice2));

      assertEquals("v2", unwrap(cl.load("k1").getValue()));

      timeService.advance(lifespan + 1);
      assertExpired(ice2, true);

      assertNull(cl.load("k1"));
   }

   public void testLoadAndStoreBytesValues() throws PersistenceException, IOException, InterruptedException {
      assertIsEmpty();

      WrappedBytes key = new WrappedByteArray(getMarshaller().objectToByteBuffer(new Pojo().role("key")));
      WrappedBytes key2 = new WrappedByteArray(getMarshaller().objectToByteBuffer(new Pojo().role("key2")));
      WrappedBytes value = new WrappedByteArray(getMarshaller().objectToByteBuffer(new Pojo().role("value")));

      assertFalse(cl.contains(key));
      cl.write(new MarshalledEntryImpl<Object, Object>(key, value, null, getMarshaller()));

      assertEquals(value, cl.load(key).getValue());
      MarshalledEntry entry = cl.load(key);
      assertTrue("Expected an immortalEntry",
                 entry.getMetadata() == null || entry.getMetadata().expiryTime() == -1 || entry.getMetadata().maxIdle() == -1);
      assertContains(key, true);

      assertFalse(cl.delete(key2));
      assertTrue(cl.delete(key));
   }

   public void testWriteAndDeleteBatch() throws Exception {
      // Number of entries is randomized to even numbers between 80 and 120
      int numberOfEntries = 2 * ThreadLocalRandom.current().nextInt(WRITE_DELETE_BATCH_MIN_ENTRIES / 2, WRITE_DELETE_BATCH_MAX_ENTRIES / 2 + 1);
      assertIsEmpty();
      assertNull("should not be present in the store", cl.load(0));
      List<MarshalledEntry<?, ?>> entries = IntStream.range(0, numberOfEntries).boxed()
            .map(i -> marshalledEntry(i.toString(), "Val" + i, null))
            .collect(Collectors.toList());

      cl.writeBatch(entries);
      Set<MarshalledEntry<Object, Object>> set = TestingUtil.allEntries(cl);
      assertSize(set, numberOfEntries);
      assertNotNull(cl.load("56"));

      int batchSize = numberOfEntries / 2;
      List<Object> keys = IntStream.range(0, batchSize).mapToObj(Integer::toString).collect(Collectors.toList());
      cl.deleteBatch(keys);
      set = TestingUtil.allEntries(cl);
      assertSize(set, batchSize);
      assertNull(cl.load("20"));
   }

   public void testEmptyBatch() {
      assertIsEmpty();

      cl.writeBatch(Collections.emptyList());
      assertSize(TestingUtil.allEntries(cl), 0);

      cl.deleteBatch(Collections.emptyList());
      assertSize(TestingUtil.allEntries(cl), 0);

      assertIsEmpty();
   }

   protected final InitializationContext createContext(Configuration configuration) {
      return PersistenceMockUtil.createContext(getClass().getSimpleName(), configuration, getMarshaller(), timeService);
   }

   protected final void assertContains(Object k, boolean expected) {
      assertEquals("contains(" + k + ")", expected, cl.contains(k));
   }

   protected final InternalCacheEntry<Object, Object> internalCacheEntry(String key, String value, long lifespan) {
      return TestInternalCacheEntryFactory.<Object, Object>create(factory, key, wrap(key, value), lifespan);
   }

   private InternalCacheEntry<Object, Object> internalCacheEntry(String key, String value, long lifespan, long idle) {
      return TestInternalCacheEntryFactory.<Object, Object>create(factory, key, wrap(key, value), lifespan, idle);
   }

   private MarshalledEntry<Object, Object> marshalledEntry(String key, String value, InternalMetadata metadata) {
      return marshalledEntry(key, wrap(key, value), metadata);
   }

   protected MarshalledEntry<Object, Object> marshalledEntry(Object key, Object value, InternalMetadata metadata) {
      return new MarshalledEntryImpl<>(key, value, metadata, getMarshaller());
   }

   protected final MarshalledEntry<Object, Object> marshalledEntry(InternalCacheEntry entry) {
      //noinspection unchecked
      return TestingUtil.marshalledEntry(entry, getMarshaller());
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



   public static class Pojo implements Serializable, ExternalPojo {

      private String role;

      public Pojo role(String role) {
         this.role = role;
         return this;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         Pojo pojo = (Pojo) o;

         if (role != null ? !role.equals(pojo.role) : pojo.role != null)
            return false;

         return true;
      }

      @Override
      public int hashCode() {
         return role != null ? role.hashCode() : 0;
      }
   }

}
