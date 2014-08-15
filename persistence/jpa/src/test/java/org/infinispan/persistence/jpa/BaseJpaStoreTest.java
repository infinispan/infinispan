package org.infinispan.persistence.jpa;

import static org.testng.Assert.*;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import javax.persistence.Persistence;

import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.marshall.core.MarshalledEntryImpl;
import org.infinispan.persistence.spi.AdvancedCacheLoader;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.util.concurrent.ConcurrentHashSet;
import org.testng.annotations.Test;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@Test(groups = "functional", testName = "persistence.BaseJpaStoreTest")
public abstract class BaseJpaStoreTest extends AbstractJpaStoreTest {
   protected abstract TestObject createTestObject(String key);

   @Test(expectedExceptions = PersistenceException.class)
   public void testStoreNoJpa() {
	   assertFalse(cs.contains("k"));
	   cs.write(createEntry("k", "v"));
   }

   @Test(expectedExceptions = PersistenceException.class)
   public void testStoreWithJpaBadKey() {
	   assertFalse(cs.contains("k"));
	   TestObject obj = createTestObject("1");
	   cs.write(createEntry("k", obj.getValue()));
   }

   public void testStoreWithJpaGoodKey() {
	   TestObject obj = createTestObject("testStoreWithJpaGoodKey");
	   assertFalse(cs.contains(obj.getKey()));
	   MarshalledEntryImpl me = createEntry(obj);
	   cs.write(me);
   }

   public void testLoadAndStoreImmortal() {
      TestObject obj = createTestObject("testLoadAndStoreImmortal");
      assertFalse(cs.contains(obj.getKey()));
      MarshalledEntryImpl me = createEntry(obj);
      cs.write(me);

      assertTrue(cs.contains(obj.getKey()));
      assertEquals(obj.getValue(), cs.load(obj.getKey()).getValue());
      assertNull(cs.load(obj.getKey()).getMetadata());

      // TODO test with metadata

      boolean removed = cs.delete("nonExistentKey");
      assertFalse(removed);
   }

   public void testPreload() throws Exception {
		TestObject obj1 = createTestObject("testPreload1");
		TestObject obj2 = createTestObject("testPreload2");
		TestObject obj3 = createTestObject("testPreload3");

		cs.write(createEntry(obj1));
		cs.write(createEntry(obj2));
		cs.write(createEntry(obj3));
      assertEquals(cs.load(obj1.getKey()).getValue(), obj1.getValue());
      assertEquals(cs.load(obj2.getKey()).getValue(), obj2.getValue());
      assertEquals(cs.load(obj3.getKey()).getValue(), obj3.getValue());

      final ConcurrentHashMap map = new ConcurrentHashMap();
      AdvancedCacheLoader.CacheLoaderTask taskWithValues = new AdvancedCacheLoader.CacheLoaderTask() {
         @Override
         public void processEntry(MarshalledEntry marshalledEntry, AdvancedCacheLoader.TaskContext taskContext) throws InterruptedException {
            if (marshalledEntry.getKey() != null && marshalledEntry.getValue() != null) {
               map.put(marshalledEntry.getKey(), marshalledEntry.getValue());
            }
         }
      };
		cs.process(null, taskWithValues, new ThreadPoolExecutor(1, 2, 1, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(10)), true, false);

		assertEquals(map.size(), 3);
      assertEquals(map.remove(obj1.getKey()), obj1.getValue());
      assertEquals(map.remove(obj2.getKey()), obj2.getValue());
      assertEquals(map.remove(obj3.getKey()), obj3.getValue());
		assertTrue(map.isEmpty());

      final ConcurrentHashSet set = new ConcurrentHashSet();
      AdvancedCacheLoader.CacheLoaderTask taskWithoutValues = new AdvancedCacheLoader.CacheLoaderTask() {
         @Override
         public void processEntry(MarshalledEntry marshalledEntry, AdvancedCacheLoader.TaskContext taskContext) throws InterruptedException {
            if (marshalledEntry.getKey() != null) {
               set.add(marshalledEntry.getKey());
            }
         }
      };
      cs.process(null, taskWithoutValues, new ThreadPoolExecutor(1, 2, 1, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(10)), false, false);
      assertEquals(set.size(), 3);
      assertTrue(set.remove(obj1.getKey()));
      assertTrue(set.remove(obj2.getKey()));
      assertTrue(set.remove(obj3.getKey()));
      assertTrue(map.isEmpty());
	}

   public void testStoreAndRemoveAll() {
		TestObject obj1 = createTestObject("testStoreAndRemoveAll1");
		TestObject obj2 = createTestObject("testStoreAndRemoveAll2");
		TestObject obj3 = createTestObject("testStoreAndRemoveAll3");
		TestObject obj4 = createTestObject("testStoreAndRemoveAll4");

		cs.write(createEntry(obj1));
      cs.write(createEntry(obj2));
      cs.write(createEntry(obj3));
      cs.write(createEntry(obj4));

		assertEquals(cs.size(), 4);

		cs.clear();
      assertEquals(cs.size(), 0);
      assertFalse(cs.contains(obj1.getKey()));
      assertFalse(cs.contains(obj2.getKey()));
      assertFalse(cs.contains(obj3.getKey()));
      assertFalse(cs.contains(obj4.getKey()));
	}

   public void testStoreValuesViaNonJpaCacheStore() {
      TestObject obj1 = createTestObject("testStoreViaNonJpaCacheStore1");
      TestObject obj2 = createTestObject("testStoreViaNonJpaCacheStore2");

      assertEquals(cs.size(), 0);
      assertFalse(cs.contains(obj1.getKey()));
      assertFalse(cs.contains(obj1.getKey()));

      EntityManagerFactory emf = Persistence.createEntityManagerFactory(PERSISTENCE_UNIT_NAME);
      EntityManager em = emf.createEntityManager();
      EntityTransaction txn = em.getTransaction();
      txn.begin();
      em.persist(obj1.getValue());
      em.persist(obj2.getValue());
      em.flush();
      txn.commit();
      em.close();

      assertEquals(cs.size(), 2);
      assertTrue(cs.contains(obj1.getKey()));
      assertTrue(cs.contains(obj1.getKey()));
   }

   public void testLoadValuesViaNonJpaCacheStore() {
      TestObject obj1 = createTestObject("testLoadViaNonJpaCacheStore1");
      TestObject obj2 = createTestObject("testLoadViaNonJpaCacheStore2");
      cs.write(createEntry(obj1));
      cs.write(createEntry(obj2));

      assertEquals(cs.size(), 2);
      assertTrue(cs.contains(obj1.getKey()));
      assertTrue(cs.contains(obj1.getKey()));

      EntityManagerFactory emf = Persistence.createEntityManagerFactory(PERSISTENCE_UNIT_NAME);
      EntityManager em = emf.createEntityManager();

      assertEquals(em.find(obj1.getValue().getClass(), obj1.getKey()), obj1.getValue());
      assertEquals(em.find(obj2.getValue().getClass(), obj2.getKey()), obj2.getValue());

      em.close();
   }


   /*

   public void testLoadAndStoreWithLifespan() throws Exception {
      assert !cs.containsKey("k");

      long lifespan = 120000;
      InternalCacheEntry se = TestInternalCacheEntryFactory.create("k", "v", lifespan);
      cs.store(se);

      assert cs.containsKey("k");
      InternalCacheEntry ice = cs.load("k");
      assertCorrectExpiry(ice, "v", lifespan, -1, false);
      ice = cs.loadAll().iterator().next();
      assertCorrectExpiry(ice, "v", lifespan, -1, false);

      lifespan = 1;
      se = TestInternalCacheEntryFactory.create("k", "v", lifespan);
      cs.store(se);
      Thread.sleep(100);
      purgeExpired();
      assert se.isExpired(System.currentTimeMillis());
      assertEventuallyExpires("k");
      assert !cs.containsKey("k");
      assert cs.loadAll().isEmpty();
   }

   private void assertCorrectExpiry(InternalCacheEntry ice, String value, long lifespan, long maxIdle, boolean expired) {
      assert ice != null : "Cache entry is null";
      assert Util.safeEquals(ice.getValue(), value) : ice.getValue() + " was not " + value;
      assert ice.getLifespan() == lifespan : ice.getLifespan() + " was not " + lifespan;
      assert ice.getMaxIdle() == maxIdle : ice.getMaxIdle() + " was not " + maxIdle;
      if (lifespan > -1) assert ice.getCreated() > -1 : "Created is -1 when maxIdle is set";
      if (maxIdle > -1) assert ice.getLastUsed() > -1 : "LastUsed is -1 when maxIdle is set";
      assert expired == ice.isExpired(System.currentTimeMillis()) : "isExpired() is not " + expired;
   }


   public void testLoadAndStoreWithIdle() throws Exception {
      assert !cs.containsKey("k");

      long idle = 120000;
      InternalCacheEntry se = TestInternalCacheEntryFactory.create("k", "v", -1, idle);
      cs.store(se);

      assert cs.containsKey("k");
      InternalCacheEntry ice = cs.load("k");
      assertCorrectExpiry(ice, "v", -1, idle, false);
      ice = cs.loadAll().iterator().next();
      assertCorrectExpiry(ice, "v", -1, idle, false);

      idle = 1;
      se = TestInternalCacheEntryFactory.create("k", "v", -1, idle);
      cs.store(se);
      Thread.sleep(100);
      purgeExpired();
      assert se.isExpired(System.currentTimeMillis());
      assertEventuallyExpires("k");
      assert !cs.containsKey("k");
      assert cs.loadAll().isEmpty();
   }

   protected void assertEventuallyExpires(String key) throws Exception {
      assert cs.load(key) == null;
   }

   protected void purgeExpired() throws CacheLoaderException {
      cs.purgeExpired();
   }

   public void testLoadAndStoreWithLifespanAndIdle() throws Exception {
      assert !cs.containsKey("k");

      long lifespan = 200000;
      long idle = 120000;
      InternalCacheEntry se = TestInternalCacheEntryFactory.create("k", "v", lifespan, idle);
      cs.store(se);

      assert cs.containsKey("k");
      InternalCacheEntry ice = cs.load("k");
      assertCorrectExpiry(ice, "v", lifespan, idle, false);
      ice = cs.loadAll().iterator().next();
      assertCorrectExpiry(ice, "v", lifespan, idle, false);

      idle = 1;
      se = TestInternalCacheEntryFactory.create("k", "v", lifespan, idle);
      cs.store(se);
      Thread.sleep(100);
      purgeExpired();
      assert se.isExpired(System.currentTimeMillis());
      assertEventuallyExpires("k");
      assert !cs.containsKey("k");
      assert cs.loadAll().isEmpty();
   }

   public void testStopStartDoesNotNukeValues() throws InterruptedException, CacheLoaderException {
      assert !cs.containsKey("k1");
      assert !cs.containsKey("k2");

      long lifespan = 1;
      long idle = 1;
      InternalCacheEntry se1 = TestInternalCacheEntryFactory.create("k1", "v1", lifespan);
      InternalCacheEntry se2 = TestInternalCacheEntryFactory.create("k2", "v2");
      InternalCacheEntry se3 = TestInternalCacheEntryFactory.create("k3", "v3", -1, idle);
      InternalCacheEntry se4 = TestInternalCacheEntryFactory.create("k4", "v4", lifespan, idle);

      cs.store(se1);
      cs.store(se2);
      cs.store(se3);
      cs.store(se4);

      cs.stop();
      cs.start();
      assert se1.isExpired(System.currentTimeMillis());
      assert cs.load("k1") == null;
      assert !cs.containsKey("k1");
      assert cs.load("k2") != null;
      assert cs.containsKey("k2");
      assert cs.load("k2").getValue().equals("v2");
      assert se3.isExpired(System.currentTimeMillis());
      assert cs.load("k3") == null;
      assert !cs.containsKey("k3");
      assert se3.isExpired(System.currentTimeMillis());
      assert cs.load("k3") == null;
      assert !cs.containsKey("k3");
   }

   protected void sleepForStopStartTest() throws InterruptedException {
      Thread.sleep(100);
   }









   public void testPurgeExpired() throws Exception {
      // Increased lifespan and idle timeouts to accommodate slower cache stores
      long lifespan = 6000;
      long idle = 4000;
      cs.store(TestInternalCacheEntryFactory.create("k1", "v1", lifespan));
      cs.store(TestInternalCacheEntryFactory.create("k2", "v2", -1, idle));
      cs.store(TestInternalCacheEntryFactory.create("k3", "v3", lifespan, idle));
      cs.store(TestInternalCacheEntryFactory.create("k4", "v4", -1, -1)); // immortal entry
      cs.store(TestInternalCacheEntryFactory.create("k5", "v5", lifespan * 1000, idle * 1000)); // long life mortal entry
      assert cs.containsKey("k1");
      assert cs.containsKey("k2");
      assert cs.containsKey("k3");
      assert cs.containsKey("k4");
      assert cs.containsKey("k5");

      Thread.sleep(lifespan + 10);
      purgeExpired();

      assert !cs.containsKey("k1");
      assert !cs.containsKey("k2");
      assert !cs.containsKey("k3");
      assert cs.containsKey("k4");
      assert cs.containsKey("k5");
   }


   public void testConfigFile() throws Exception {
      Class<? extends CacheLoaderConfig> cfgClass = cs.getConfigurationClass();
      CacheLoaderConfig clc = Util.getInstance(cfgClass);
      assert clc.getCacheLoaderClassName().equals(cs.getClass().getName()) : "Cache loaders doesn't provide a proper configuration type that is capable of creating the loaders!";
   }



   public void testReplaceExpiredEntry() throws Exception {
      final long startTime = System.currentTimeMillis();
      final long lifespan = 3000;
      cs.store(TestInternalCacheEntryFactory.create("k1", "v1", lifespan));
      while (true) {
         InternalCacheEntry entry = cs.load("k1");
         if (System.currentTimeMillis() >= startTime + lifespan)
            break;
         assert entry.getValue().equals("v1");
         Thread.sleep(100);
      }

      // Make sure that in the next 20 secs data is removed
      while (System.currentTimeMillis() < startTime + lifespan + 20000) {
         if (cs.load("k1") == null) break;
      }

      assert null == cs.load("k1");

      cs.store(TestInternalCacheEntryFactory.create("k1", "v2", lifespan));
      while (true) {
         InternalCacheEntry entry = cs.load("k1");
         if (System.currentTimeMillis() >= startTime + lifespan)
            break;
         assert entry.getValue().equals("v2");
         Thread.sleep(100);
      }

      // Make sure that in the next 20 secs data is removed
      while (System.currentTimeMillis() < startTime + lifespan + 20000) {
         if (cs.load("k1") == null) break;
      }

      assert null == cs.load("k1");
   }

   public void testLoadAndStoreMarshalledValues() throws CacheLoaderException {
      MarshalledValue key = new MarshalledValue(new Pojo().role("key"), true, getMarshaller());
      MarshalledValue key2 = new MarshalledValue(new Pojo().role("key2"), true, getMarshaller());
      MarshalledValue value = new MarshalledValue(new Pojo().role("value"), true, getMarshaller());

      assert !cs.containsKey(key);
      InternalCacheEntry se = TestInternalCacheEntryFactory.create(key, value);
      cs.store(se);

      assert cs.load(key).getValue().equals(value);
      assert cs.load(key).getLifespan() == -1;
      assert cs.load(key).getMaxIdle() == -1;
      assert !cs.load(key).isExpired(System.currentTimeMillis());
      assert cs.containsKey(key);

      boolean removed = cs.remove(key2);
      assert !removed;

      assert cs.remove(key);
   }
   */
}
