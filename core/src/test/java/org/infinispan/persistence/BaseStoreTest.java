package org.infinispan.persistence;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.equivalence.AnyEquivalence;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.TestObjectStreamMarshaller;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.marshall.core.MarshalledEntryImpl;
import org.infinispan.marshall.core.MarshalledValue;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestInternalCacheEntryFactory;
import org.infinispan.transaction.xa.TransactionFactory;
import org.infinispan.util.concurrent.WithinThreadExecutor;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static java.util.Collections.emptySet;
import static org.infinispan.persistence.PersistenceUtil.internalMetadata;
import static org.infinispan.test.TestingUtil.allEntries;
import static org.infinispan.test.TestingUtil.marshalledEntry;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;

/**
 * This is a base class containing various unit tests for each and every different CacheStore implementations. If you
 * need to add Cache/CacheManager tests that need to be run for each cache store/loader implementation, then use
 * BaseStoreFunctionalTest.
 */
@SuppressWarnings("unchecked")
// this needs to be here for the test to run in an IDE
@Test(groups = "unit", testName = "persistence.BaseStoreTest")
public abstract class BaseStoreTest extends AbstractInfinispanTest {

   private TestObjectStreamMarshaller marshaller;

   protected abstract AdvancedLoadWriteStore createStore() throws Exception;

   protected AdvancedLoadWriteStore cl;
   protected StoreConfiguration csc;

   protected TransactionFactory gtf = new TransactionFactory();

   protected BaseStoreTest() {
      gtf.init(false, false, true, false);
   }

   //alwaysRun = true otherwise, when we run unstable tests, this method is not invoked (because it belongs to the unit group)
   @BeforeMethod(alwaysRun = true)
   public void setUp() throws Exception {
      marshaller = new TestObjectStreamMarshaller();
      try {
         cl = createStore();
      } catch (Exception e) {
         //in IDEs this won't be printed which makes debugging harder
         e.printStackTrace();
         throw e;
      }
   }

   //alwaysRun = true otherwise, when we run unstable tests, this method is not invoked (because it belongs to the unit group)
   @AfterMethod(alwaysRun = true)
   protected void stopMarshaller() {
      if (marshaller != null) {
         marshaller.stop();
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
    * @param value
    * @return
    */
   protected Object wrap(String key, String value) {
      return value;
   }

   /**
    * Overridden in stores which accept only certain value types
    * @param wrapped
    * @return
    */
   protected String unwrap(Object wrapped) {
      return (String) wrapped;
   }

   public void testLoadAndStoreImmortal() throws PersistenceException {
      assertFalse(cl.contains("k"));
      cl.write(new MarshalledEntryImpl("k", wrap("k", "v"), null, getMarshaller()));

      assertEquals("v", unwrap(cl.load("k").getValue()));
      assert cl.load("k").getMetadata() == null || cl.load("k").getMetadata().expiryTime() == -1;
      assert cl.load("k").getMetadata() == null || cl.load("k").getMetadata().maxIdle() == -1;
      assert cl.contains("k");

      boolean removed = cl.delete("k2");
      assertFalse(removed);
   }

   public void testLoadAndStoreWithLifespan() throws Exception {
      assertFalse(cl.contains("k"));

      long lifespan = 120000;
      InternalCacheEntry se = TestInternalCacheEntryFactory.create("k", wrap("k", "v"), lifespan);
      cl.write(new MarshalledEntryImpl("k", wrap("k", "v"), internalMetadata(se), getMarshaller()));

      assert cl.contains("k");
      MarshalledEntry me = cl.load("k");
      assertCorrectExpiry(me, "v", lifespan, -1, false);

      me = TestingUtil.allEntries(cl).iterator().next();
      assertCorrectExpiry(me, "v", lifespan, -1, false);

      lifespan = 1;
      se = TestInternalCacheEntryFactory.create("k", wrap("k", "v"), lifespan);
      cl.write(new MarshalledEntryImpl("k", wrap("k", "v"), internalMetadata(se), getMarshaller()));
      Thread.sleep(100);
      purgeExpired();
      assert se.isExpired(System.currentTimeMillis());
      assertEventuallyExpires("k");
      assertFalse(cl.contains("k"));
      assert TestingUtil.allEntries(cl).isEmpty();
   }

   private void assertCorrectExpiry(MarshalledEntry me, String value, long lifespan, long maxIdle, boolean expired) {
      assertNotNull(me);
      assertEquals(value, unwrap(me.getValue()));

      if (lifespan > -1) {
         assert me.getMetadata().lifespan() == lifespan : me.getMetadata().lifespan() + " was not " + lifespan;
         assert me.getMetadata().created() > -1 : "Created is -1 when maxIdle is set";
      }
      if (maxIdle > -1) {
         assert me.getMetadata().maxIdle() == maxIdle : me.getMetadata().maxIdle() + " was not " + maxIdle;
         assert me.getMetadata().lastUsed() > -1 : "LastUsed is -1 when maxIdle is set";
      }
      if (me.getMetadata() != null) {
         assert expired == me.getMetadata().isExpired(System.currentTimeMillis()) : "isExpired() is not " + expired;
      }
   }


   public void testLoadAndStoreWithIdle() throws Exception {
      assertFalse(cl.contains("k"));

      long idle = 120000;
      InternalCacheEntry se = TestInternalCacheEntryFactory.create("k", wrap("k", "v"), -1, idle);
      cl.write(marshalledEntry(se, getMarshaller()));

      assert cl.contains("k");
      MarshalledEntry me = cl.load("k");
      assertCorrectExpiry(me, "v", -1, idle, false);
      assertCorrectExpiry(TestingUtil.allEntries(cl).iterator().next(), "v", -1, idle, false);

      idle = 1;
      se = TestInternalCacheEntryFactory.create("k", wrap("k", "v"), -1, idle);
      cl.write(marshalledEntry(se, getMarshaller()));
      Thread.sleep(100);
      purgeExpired();
      assert se.isExpired(System.currentTimeMillis());
      assertEventuallyExpires("k");
      assertFalse(cl.contains("k"));
      assertIsEmpty();
   }

   private void assertIsEmpty() {
      assert TestingUtil.allEntries(cl).isEmpty();
   }

   protected void assertEventuallyExpires(final String key) throws Exception {
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return cl.load(key) == null;
         }
      });
   }

   protected void purgeExpired() {
      cl.purge(new WithinThreadExecutor(), null);
   }

   public void testLoadAndStoreWithLifespanAndIdle() throws Exception {
      assertFalse(cl.contains("k"));

      long lifespan = 200000;
      long idle = 120000;
      InternalCacheEntry se = TestInternalCacheEntryFactory.create("k", wrap("k", "v"), lifespan, idle);
      InternalCacheValue icv = se.toInternalCacheValue();
      assertEquals(se.getCreated(), icv.getCreated());
      assertEquals(se.getLastUsed(), icv.getLastUsed());
      cl.write(marshalledEntry(se, getMarshaller()));

      assert cl.contains("k");
      MarshalledEntry ice = cl.load("k");
      assertCorrectExpiry(ice, "v", lifespan, idle, false);
      assertCorrectExpiry(TestingUtil.allEntries(cl).iterator().next(), "v", lifespan, idle, false);

      idle = 1;
      se = TestInternalCacheEntryFactory.create("k", wrap("k", "v"), lifespan, idle);
      cl.write(marshalledEntry(se, getMarshaller()));
      Thread.sleep(100);
      purgeExpired();
      assert se.isExpired(System.currentTimeMillis());
      assertEventuallyExpires("k");
      assertFalse(cl.contains("k"));
      assertIsEmpty();
   }

   public void testStopStartDoesNotNukeValues() throws InterruptedException, PersistenceException {
      assertFalse(cl.contains("k1"));
      assertFalse(cl.contains("k2"));

      long lifespan = 1;
      long idle = 1;
      InternalCacheEntry se1 = TestInternalCacheEntryFactory.create("k1", wrap("k1", "v1"), lifespan);
      InternalCacheEntry se2 = TestInternalCacheEntryFactory.create("k2", wrap("k2", "v2"));
      InternalCacheEntry se3 = TestInternalCacheEntryFactory.create("k3", wrap("k3", "v3"), -1, idle);
      InternalCacheEntry se4 = TestInternalCacheEntryFactory.create("k4", wrap("k4", "v4"), lifespan, idle);

      cl.write(marshalledEntry(se1, getMarshaller()));
      cl.write(marshalledEntry(se2, getMarshaller()));
      cl.write(marshalledEntry(se3, getMarshaller()));
      cl.write(marshalledEntry(se4, getMarshaller()));

      sleepForStopStartTest();

      cl.stop();
      cl.start();
      assert se1.isExpired(System.currentTimeMillis());
      assertNull(cl.load("k1"));
      assertFalse(cl.contains("k1"));
      assertNotNull(cl.load("k2"));
      assert cl.contains("k2");
      assertEquals("v2", unwrap(cl.load("k2").getValue()));
      assert se3.isExpired(System.currentTimeMillis());
      assertNull(cl.load("k3"));
      assertFalse(cl.contains("k3"));
      assert se3.isExpired(System.currentTimeMillis());
      assertNull(cl.load("k3"));
      assertFalse(cl.contains("k3"));
   }

   protected void sleepForStopStartTest() throws InterruptedException {
      Thread.sleep(100);
   }

   public void testPreload() throws Exception {
      cl.write(new MarshalledEntryImpl("k1", wrap("k1", "v1"), null, getMarshaller()));
      cl.write(new MarshalledEntryImpl("k2", wrap("k2", "v2"), null, getMarshaller()));
      cl.write(new MarshalledEntryImpl("k3", wrap("k3", "v3"), null, getMarshaller()));

      Set<MarshalledEntry> set = TestingUtil.allEntries(cl);

      assertEquals(3, set.size());
      Set expected = new HashSet();
      expected.add("k1");
      expected.add("k2");
      expected.add("k3");
      for (MarshalledEntry se : set)
         assertTrue(expected.remove(se.getKey()));
      assertTrue(expected.isEmpty());
   }

   public void testStoreAndRemove() throws PersistenceException {
      cl.write(new MarshalledEntryImpl("k1", wrap("k1", "v1"), null, getMarshaller()));
      cl.write(new MarshalledEntryImpl("k2", wrap("k2", "v2"), null, getMarshaller()));
      cl.write(new MarshalledEntryImpl("k3", wrap("k3", "v3"), null, getMarshaller()));
      cl.write(new MarshalledEntryImpl("k4", wrap("k4", "v4"), null, getMarshaller()));


      Set<MarshalledEntry> set = TestingUtil.allEntries(cl);

      assert set.size() == 4;
      Set expected = new HashSet();
      expected.add("k1");
      expected.add("k2");
      expected.add("k3");
      expected.add("k4");
      for (MarshalledEntry se : set) assert expected.remove(se.getKey());
      assert expected.isEmpty();

      cl.delete("k1");
      cl.delete("k2");
      cl.delete("k3");

      set = TestingUtil.allEntries(cl);
      assert set.size() == 1;
      set.remove("k4");
      assert expected.isEmpty();
   }

   public void testPurgeExpired() throws Exception {
      // Increased lifespan and idle timeouts to accommodate slower cache stores
      long lifespan = 6000;
      long idle = 4000;
      InternalCacheEntry ice1 = TestInternalCacheEntryFactory.create("k1", wrap("k1", "v1"), lifespan);
      cl.write(marshalledEntry(ice1, getMarshaller()));
      InternalCacheEntry ice2 = TestInternalCacheEntryFactory.create("k2", wrap("k2", "v2"), -1, idle);
      cl.write(marshalledEntry(ice2, getMarshaller()));
      InternalCacheEntry ice3 = TestInternalCacheEntryFactory.create("k3", wrap("k3", "v3"), lifespan, idle);
      cl.write(marshalledEntry(ice3, getMarshaller()));
      InternalCacheEntry ice4 = TestInternalCacheEntryFactory.create("k4", wrap("k4", "v4"), -1, -1);
      cl.write(marshalledEntry(ice4, getMarshaller())); // immortal entry
      InternalCacheEntry ice5 = TestInternalCacheEntryFactory.create("k5", wrap("k5", "v5"), lifespan * 1000, idle * 1000);
      cl.write(marshalledEntry(ice5, getMarshaller())); // long life mortal entry
      assert cl.contains("k1");
      assert cl.contains("k2");
      assert cl.contains("k3");
      assert cl.contains("k4");
      assert cl.contains("k5");

      Thread.sleep(lifespan + 10);
      purgeExpired();

      assertFalse(cl.contains("k1"));
      assertFalse(cl.contains("k2"));
      assertFalse(cl.contains("k3"));
      assert cl.contains("k4");
      assert cl.contains("k5");
   }

   public void testLoadAll() throws PersistenceException {

      cl.write(new MarshalledEntryImpl("k1", wrap("k1", "v1"), null, getMarshaller()));
      cl.write(new MarshalledEntryImpl("k2", wrap("k2", "v2"), null, getMarshaller()));
      cl.write(new MarshalledEntryImpl("k3", wrap("k3", "v3"), null, getMarshaller()));
      cl.write(new MarshalledEntryImpl("k4", wrap("k4", "v4"), null, getMarshaller()));
      cl.write(new MarshalledEntryImpl("k5", wrap("k5", "v5"), null, getMarshaller()));

      Set<MarshalledEntry> s = TestingUtil.allEntries(cl);
      assert s.size() == 5 : "Expected 5 keys, was " + s;

      s = allEntries(cl, new CollectionKeyFilter(emptySet()));
      assert s.size() == 5 : "Expected 5 keys, was " + s;

      s = allEntries(cl, new CollectionKeyFilter(Collections.<Object>singleton("k3")));
      assert s.size() == 4 : "Expected 4 keys but was " + s;

      for (MarshalledEntry me: s)
         assertFalse(me.getKey().equals("k3"));
   }

   public void testReplaceExpiredEntry() throws Exception {
      final long startTime = System.currentTimeMillis();
      final long lifespan = 3000;
      InternalCacheEntry ice = TestInternalCacheEntryFactory.create("k1", wrap("k1", "v1"), lifespan);
      cl.write(marshalledEntry(ice, getMarshaller()));
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

      assert null == cl.load("k1");

      InternalCacheEntry ice2 = TestInternalCacheEntryFactory.create("k1", wrap("k1", "v2"), lifespan);
      cl.write(marshalledEntry(ice2, getMarshaller()));
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

      assert null == cl.load("k1");
   }

   public void testLoadAndStoreMarshalledValues() throws PersistenceException {
      MarshalledValue key = new MarshalledValue(new Pojo().role("key"), getMarshaller());
      MarshalledValue key2 = new MarshalledValue(new Pojo().role("key2"), getMarshaller());
      MarshalledValue value = new MarshalledValue(new Pojo().role("value"), getMarshaller());

      assertFalse(cl.contains(key));
      cl.write(new MarshalledEntryImpl(key, value, null, getMarshaller()));

      assertEquals(value, cl.load(key).getValue());
      assert cl.load(key).getMetadata() == null || cl.load(key).getMetadata().expiryTime() == - 1;
      assert cl.load(key).getMetadata() == null || cl.load(key).getMetadata().lifespan() == - 1;
      assert cl.contains(key);

      boolean removed = cl.delete(key2);
      assertFalse(removed);

      assert cl.delete(key);
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

   /**
    * @return a mock cache for use with the cache store impls
    */
   protected Cache getCache() {
      String name = "mockCache-" + getClass().getName();
      return mockCache(name);
   }

   public static Cache mockCache(String name) {
      AdvancedCache cache = mock(AdvancedCache.class);
      Configuration config = new ConfigurationBuilder()
                                    .dataContainer()
                                    .keyEquivalence(AnyEquivalence.getInstance())
                                    .valueEquivalence(AnyEquivalence.getInstance())
                                    .build();

      GlobalConfiguration gc = new GlobalConfigurationBuilder().build();

      Set<String> cachesSet = new HashSet<String>();
      EmbeddedCacheManager cm = mock(EmbeddedCacheManager.class);
      GlobalComponentRegistry gcr = new GlobalComponentRegistry(gc, cm, cachesSet);
      ComponentRegistry registry = new ComponentRegistry("cache", config, cache, gcr, BaseStoreTest.class.getClassLoader());

      when(cache.getName()).thenReturn(name);
      when(cache.getAdvancedCache()).thenReturn(cache);
      when(cache.getComponentRegistry()).thenReturn(registry);
      when(cache.getStatus()).thenReturn(ComponentStatus.RUNNING);
      when(cache.getCacheConfiguration()).thenReturn(config);
      return cache;
   }
}
