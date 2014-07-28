package org.infinispan.persistence;

import static java.util.Collections.emptySet;
import static org.infinispan.test.TestingUtil.allEntries;
import static org.testng.AssertJUnit.*;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.infinispan.filter.CollectionKeyFilter;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.marshall.TestObjectStreamMarshaller;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.marshall.core.MarshalledEntryImpl;
import org.infinispan.marshall.core.MarshalledValue;
import org.infinispan.metadata.InternalMetadata;
import org.infinispan.persistence.spi.AdvancedCacheWriter;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestInternalCacheEntryFactory;
import org.infinispan.util.PersistenceMockUtil;
import org.infinispan.util.concurrent.ConcurrentHashSet;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * This is a base class containing various unit tests for each and every different CacheStore implementations. If you
 * need to add Cache/CacheManager tests that need to be run for each cache store/loader implementation, then use
 * BaseStoreFunctionalTest.
 */
// this needs to be here for the test to run in an IDE
@Test(groups = "unit", testName = "persistence.BaseStoreTest")
public abstract class BaseStoreTest extends AbstractInfinispanTest {

   private TestObjectStreamMarshaller marshaller;
   protected abstract AdvancedLoadWriteStore createStore() throws Exception;

   protected AdvancedLoadWriteStore<Object, Object> cl;

   //alwaysRun = true otherwise, when we run unstable tests, this method is not invoked (because it belongs to the unit group)
   @BeforeMethod(alwaysRun = true)
   public void setUp() throws Exception {
      marshaller = new TestObjectStreamMarshaller();
      try {
         //noinspection unchecked
         cl = createStore();
         cl.start();
      } catch (Exception e) {
         //in IDEs this won't be printed which makes debugging harder
         e.printStackTrace();
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
      InternalCacheEntry se = TestInternalCacheEntryFactory.create("k", wrap("k", "v"), lifespan);
      cl.write(marshalledEntry(se));

      assertContains("k", true);
      MarshalledEntry me = cl.load("k");
      assertCorrectExpiry(me, "v", lifespan, -1, false);

      me = TestingUtil.allEntries(cl).iterator().next();
      assertCorrectExpiry(me, "v", lifespan, -1, false);

      lifespan = 1;
      se = TestInternalCacheEntryFactory.create("k", wrap("k", "v"), lifespan);
      cl.write(marshalledEntry(se));
      Thread.sleep(100);
      purgeExpired(Collections.singleton("k"), 10000);
      assertExpired(se, true);
      assertEventuallyExpires("k");
      assertContains("k", false);
      assertIsEmpty();
   }

   private void assertCorrectExpiry(MarshalledEntry me, String value, long lifespan, long maxIdle, boolean expired) {
      assertNotNull(me);
      assertEquals(value, unwrap(me.getValue()));

      if (lifespan > -1) {
         assertEquals("Wrong lifespan for entry." + me, lifespan, me.getMetadata().lifespan());
         assertTrue("Created is not -1 when lifespan is used.", me.getMetadata().created() > -1);
      }
      if (maxIdle > -1) {
         assertEquals("Wrong maxIdle for entry." + me, maxIdle, me.getMetadata().maxIdle());
         assertTrue("LastUsed is not -1 when maxIdle is used.", me.getMetadata().lastUsed() > -1);
      }
      if (me.getMetadata() != null) {
         assertEquals(me + ".isExpired() ", expired, me.getMetadata().isExpired(System.currentTimeMillis()));
      }
   }


   public void testLoadAndStoreWithIdle() throws Exception {
      assertIsEmpty();

      long idle = 120000;
      InternalCacheEntry se = TestInternalCacheEntryFactory.create("k", wrap("k", "v"), -1, idle);
      cl.write(marshalledEntry(se));

      assertContains("k", true);
      MarshalledEntry me = cl.load("k");
      assertCorrectExpiry(me, "v", -1, idle, false);
      assertCorrectExpiry(TestingUtil.allEntries(cl).iterator().next(), "v", -1, idle, false);

      idle = 1;
      se = TestInternalCacheEntryFactory.create("k", wrap("k", "v"), -1, idle);
      cl.write(marshalledEntry(se));
      Thread.sleep(100);
      purgeExpired(Collections.singleton("k"), 10000);
      assertExpired(se, true);
      assertEventuallyExpires("k");
      assertContains("k", false);
      assertIsEmpty();
   }

   private void assertIsEmpty() {
      assertEmpty(TestingUtil.allEntries(cl), true);
   }

   protected void assertEventuallyExpires(final String key) throws Exception {
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return cl.load(key) == null;
         }
      });
   }

   /* Override if the store cannot purge all expired entries upon request */
   protected boolean storePurgesAllExpired() {
      return true;
   }

   protected void purgeExpired(Collection<String> expiredKeys, long timeout) {
      final ThreadPoolExecutor executor = new ThreadPoolExecutor(3, 3, 1, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
      final Set<String> expired = new ConcurrentHashSet<>();
      for (String key : expiredKeys) // addAll is not supported
         expired.add(key);
      final Set<Object> incorrect = new ConcurrentHashSet<>();
      final AdvancedCacheWriter.PurgeListener purgeListener = new AdvancedCacheWriter.PurgeListener<String>() {
         @Override
         public void entryPurged(String key) {
            if (!expired.remove(key)) {
               incorrect.add(key);
            }
         }
      };

      long start = System.nanoTime();
      for (;;) {
         // purge is executed by eviction thread, which is different than application thread
         // this may matter for some locking cases
         try {
            executor.submit(new Callable<Void>() {
               @Override
               public Void call() throws Exception {
                  //noinspection unchecked
                  cl.purge(executor, purgeListener);
                  return null;
               }
            }).get();
         } catch (Exception e) {
            throw new RuntimeException("Purge has thrown an exception", e);
         }
         assertEquals(Collections.emptySet(), incorrect);
         if (expired.isEmpty() || !storePurgesAllExpired()) {
            return;
         }
         if (System.nanoTime() > start + TimeUnit.MILLISECONDS.toNanos(timeout)) {
            throw new IllegalStateException("Purge has timed out");
         } else {
            Thread.yield();
         }
      }
   }

   public void testLoadAndStoreWithLifespanAndIdle() throws Exception {
      assertIsEmpty();

      long lifespan = 200000;
      long idle = 120000;
      InternalCacheEntry se = TestInternalCacheEntryFactory.create("k", wrap("k", "v"), lifespan, idle);
      InternalCacheValue icv = se.toInternalCacheValue();
      assertEquals(se.getCreated(), icv.getCreated());
      assertEquals(se.getLastUsed(), icv.getLastUsed());
      cl.write(marshalledEntry(se));

      assertContains("k", true);
      MarshalledEntry ice = cl.load("k");
      assertCorrectExpiry(ice, "v", lifespan, idle, false);
      assertCorrectExpiry(TestingUtil.allEntries(cl).iterator().next(), "v", lifespan, idle, false);

      idle = 1;
      se = TestInternalCacheEntryFactory.create("k", wrap("k", "v"), lifespan, idle);
      cl.write(marshalledEntry(se));
      Thread.sleep(100);
      purgeExpired(Collections.singleton("k"), 10000);
      assertExpired(se, true);
      assertEventuallyExpires("k");
      assertContains("k", false);
      assertIsEmpty();
   }

   public void testStopStartDoesNotNukeValues() throws InterruptedException, PersistenceException {
      assertIsEmpty();

      long lifespan = 1;
      long idle = 1;
      InternalCacheEntry se1 = TestInternalCacheEntryFactory.create("k1", wrap("k1", "v1"), lifespan);
      InternalCacheEntry se2 = TestInternalCacheEntryFactory.create("k2", wrap("k2", "v2"));
      InternalCacheEntry se3 = TestInternalCacheEntryFactory.create("k3", wrap("k3", "v3"), -1, idle);
      InternalCacheEntry se4 = TestInternalCacheEntryFactory.create("k4", wrap("k4", "v4"), lifespan, idle);

      cl.write(marshalledEntry(se1));
      cl.write(marshalledEntry(se2));
      cl.write(marshalledEntry(se3));
      cl.write(marshalledEntry(se4));

      sleepForStopStartTest();

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

   protected void sleepForStopStartTest() throws InterruptedException {
      Thread.sleep(100);
   }

   public void testPreload() throws Exception {
      assertIsEmpty();

      cl.write(marshalledEntry("k1", "v1", null));
      cl.write(marshalledEntry("k2", "v2", null));
      cl.write(marshalledEntry("k3", "v3", null));

      Set<MarshalledEntry> set = TestingUtil.allEntries(cl);

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


      Set<MarshalledEntry> set = TestingUtil.allEntries(cl);

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
   }

   public void testPurgeExpired() throws Exception {
      assertIsEmpty();
      // Increased lifespan and idle timeouts to accommodate slower cache stores
      long lifespan = 6000;
      long idle = 4000;
      InternalCacheEntry ice1 = TestInternalCacheEntryFactory.create("k1", wrap("k1", "v1"), lifespan);
      cl.write(marshalledEntry(ice1));
      InternalCacheEntry ice2 = TestInternalCacheEntryFactory.create("k2", wrap("k2", "v2"), -1, idle);
      cl.write(marshalledEntry(ice2));
      InternalCacheEntry ice3 = TestInternalCacheEntryFactory.create("k3", wrap("k3", "v3"), lifespan, idle);
      cl.write(marshalledEntry(ice3));
      InternalCacheEntry ice4 = TestInternalCacheEntryFactory.create("k4", wrap("k4", "v4"), -1, -1);
      cl.write(marshalledEntry(ice4)); // immortal entry
      InternalCacheEntry ice5 = TestInternalCacheEntryFactory.create("k5", wrap("k5", "v5"), lifespan * 1000, idle * 1000);
      cl.write(marshalledEntry(ice5)); // long life mortal entry
      assertContains("k1", true);
      assertContains("k2", true);
      assertContains("k3", true);
      assertContains("k4", true);
      assertContains("k5", true);

      Thread.sleep(lifespan + 10);

      HashSet<String> expiredKeys = new HashSet<>(Arrays.asList("k1", "k2", "k3"));
      purgeExpired(Collections.unmodifiableSet(expiredKeys), 10000);

      for (String key : expiredKeys) {
         assertContains(key, false);
      }
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

      Set<MarshalledEntry> s = TestingUtil.allEntries(cl);
      assertSize(s, 5);

      s = allEntries(cl, new CollectionKeyFilter<>(emptySet()));
      assertSize(s, 5);

      s = allEntries(cl, new CollectionKeyFilter<>(Collections.<Object>singleton("k3")));
      assertSize(s, 4);

      for (MarshalledEntry me : s) {
         assertFalse(me.getKey().equals("k3"));
      }
   }

   public void testReplaceExpiredEntry() throws Exception {
      assertIsEmpty();
      final long startTime = System.currentTimeMillis();
      final long lifespan = 3000;
      InternalCacheEntry ice = TestInternalCacheEntryFactory.create("k1", wrap("k1", "v1"), lifespan);
      cl.write(marshalledEntry(ice));
      while (true) {
         MarshalledEntry entry = cl.load("k1");
         if (System.currentTimeMillis() >= startTime + lifespan)
            break;
         assertEquals("v1", unwrap(entry.getValue()));
         Thread.sleep(100);
      }

      // Make sure that in the next 20 secs data is removed
      while (System.currentTimeMillis() < startTime + lifespan + 20000) {
         if (cl.load("k1") == null) break;
      }

      assertNull(cl.load("k1"));

      InternalCacheEntry ice2 = TestInternalCacheEntryFactory.create("k1", wrap("k1", "v2"), lifespan);
      cl.write(marshalledEntry(ice2));
      while (true) {
         MarshalledEntry entry = cl.load("k1");
         if (System.currentTimeMillis() >= startTime + lifespan)
            break;
         assertEquals("v2", unwrap(entry.getValue()));
         Thread.sleep(100);
      }

      // Make sure that in the next 20 secs data is removed
      while (System.currentTimeMillis() < startTime + lifespan + 20000) {
         if (cl.load("k1") == null) break;
      }

      assertNull(cl.load("k1"));
   }

   public void testLoadAndStoreMarshalledValues() throws PersistenceException {
      assertIsEmpty();

      MarshalledValue key = new MarshalledValue(new Pojo().role("key"), getMarshaller());
      MarshalledValue key2 = new MarshalledValue(new Pojo().role("key2"), getMarshaller());
      MarshalledValue value = new MarshalledValue(new Pojo().role("value"), getMarshaller());

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

   protected final InitializationContext createContext(Configuration configuration) {
      return PersistenceMockUtil.createContext(getClass().getSimpleName(), configuration, getMarshaller());
   }

   protected final void assertContains(Object k, boolean expected) {
      assertEquals("contains(" + k + ")", expected, cl.contains(k));
   }

   private MarshalledEntry<Object, Object> marshalledEntry(String key, String value, InternalMetadata metadata) {
      return new MarshalledEntryImpl<Object, Object>(key, wrap(key, value), metadata, getMarshaller());
   }

   private MarshalledEntry<Object, Object> marshalledEntry(InternalCacheEntry entry) {
      //noinspection unchecked
      return TestingUtil.marshalledEntry(entry, getMarshaller());
   }

   private void assertSize(Collection<?> collection, int expected) {
      assertEquals(collection + ".size()", expected, collection.size());
   }

   private void assertExpired(InternalCacheEntry entry, boolean expected) {
      assertEquals(entry + ".isExpired() ", expected, entry.isExpired(System.currentTimeMillis()));
   }

   private void assertEmpty(Collection<?> collection, boolean expected) {
      assertEquals(collection + ".isEmpty()", expected, collection.isEmpty());
   }



   public static class Pojo implements Serializable {

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
