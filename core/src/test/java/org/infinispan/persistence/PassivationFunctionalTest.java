package org.infinispan.persistence;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.manager.CacheContainer;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import java.util.HashMap;
import java.util.Map;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Tests the interceptor chain and surrounding logic
 *
 * @author Manik Surtani
 */
@Test(groups = "functional", testName = "persistence.PassivationFunctionalTest")
public class PassivationFunctionalTest extends AbstractInfinispanTest {
   Cache cache;
   AdvancedLoadWriteStore store;
   TransactionManager tm;
   ConfigurationBuilder cfg;
   CacheContainer cm;
   long lifespan = 6000000; // very large lifespan so nothing actually expires

   @BeforeTest
   public void setUp() {
      cfg = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      cfg
         .persistence()
            .passivation(true)
               .addStore(DummyInMemoryStoreConfigurationBuilder.class);

      cm = TestCacheManagerFactory.createCacheManager(cfg);
      cache = cm.getCache();
      store = (AdvancedLoadWriteStore) TestingUtil.getCacheLoader(cache);
      tm = TestingUtil.getTransactionManager(cache);
   }

   @AfterTest
   public void tearDown() {
      TestingUtil.killCacheManagers(cm);
   }

   @AfterMethod
   public void afterMethod() throws PersistenceException {
      if (cache != null) cache.clear();
      if (store != null) store.clear();
   }

   private void assertInCacheNotInStore(Object key, Object value) throws PersistenceException {
      assertInCacheNotInStore(key, value, -1);
   }

   private void assertInCacheNotInStore(Object key, Object value, long lifespanMillis) throws PersistenceException {
      InternalCacheValue se = cache.getAdvancedCache().getDataContainer().get(key).toInternalCacheValue();
      testStoredEntry(se, value, lifespanMillis, "Cache", key);
      assert !store.contains(key) : "Key " + key + " should not be in store!";
   }

   private void assertInStoreNotInCache(Object key, Object value) throws PersistenceException {
      assertInStoreNotInCache(key, value, -1);
   }

   private void assertInStoreNotInCache(Object key, Object value, long lifespanMillis) throws PersistenceException {
      MarshalledEntry se = store.load(key);
      testStoredEntry(se, value, lifespanMillis, "Store", key);
      assert !cache.getAdvancedCache().getDataContainer().containsKey(key) : "Key " + key + " should not be in cache!";
   }


   private void testStoredEntry(InternalCacheValue entry, Object expectedValue, long expectedLifespan, String src, Object key) {
      assert entry != null : src + " entry for key " + key + " should NOT be null";
      assert entry.getValue().equals(expectedValue) : src + " should contain value " + expectedValue + " under key " + key + " but was " + entry.getValue() + ". Entry is " + entry;
      assert entry.getLifespan() == expectedLifespan : src + " expected lifespan for key " + key + " to be " + expectedLifespan + " but was " + entry.getLifespan() + ". Entry is " + entry;
   }

   private void testStoredEntry(MarshalledEntry entry, Object expectedValue, long expectedLifespan, String src, Object key) {
      assert entry != null : src + " entry for key " + key + " should NOT be null";
      assert entry.getValue().equals(expectedValue) : src + " should contain value " + expectedValue + " under key " + key + " but was " + entry.getValue() + ". Entry is " + entry;
      if (expectedLifespan > -1)
      assert entry.getMetadata().lifespan() == expectedLifespan : src + " expected lifespan for key " + key + " to be " + expectedLifespan + " but was " + entry.getMetadata().lifespan() + ". Entry is " + entry;
   }

   private void assertNotInCacheAndStore(Object... keys) throws PersistenceException {
      for (Object key : keys) {
         assert !cache.getAdvancedCache().getDataContainer().containsKey(key) : "Cache should not contain key " + key;
         assert !store.contains(key) : "Store should not contain key " + key;
      }
   }

   public void testPassivate() throws PersistenceException {
      assertNotInCacheAndStore("k1", "k2");

      cache.put("k1", "v1");
      cache.put("k2", "v2", lifespan, MILLISECONDS);

      assertInCacheNotInStore("k1", "v1");
      assertInCacheNotInStore("k2", "v2", lifespan);

      cache.evict("k1");
      cache.evict("k2");

      assertInStoreNotInCache("k1", "v1");
      assertInStoreNotInCache("k2", "v2", lifespan);

      // now activate

      assert cache.get("k1").equals("v1");
      assert cache.get("k2").equals("v2");

      assertInCacheNotInStore("k1", "v1");
      assertInCacheNotInStore("k2", "v2", lifespan);

      cache.evict("k1");
      cache.evict("k2");

      assertInStoreNotInCache("k1", "v1");
      assertInStoreNotInCache("k2", "v2", lifespan);
   }

   public void testRemoveAndReplace() throws PersistenceException {
      assertNotInCacheAndStore("k1", "k2");

      cache.put("k1", "v1");
      cache.put("k2", "v2", lifespan, MILLISECONDS);

      assertInCacheNotInStore("k1", "v1");
      assertInCacheNotInStore("k2", "v2", lifespan);

      cache.evict("k1");
      cache.evict("k2");

      assertInStoreNotInCache("k1", "v1");
      assertInStoreNotInCache("k2", "v2", lifespan);

      assert cache.remove("k1").equals("v1");
      assertNotInCacheAndStore("k1");

      assert cache.put("k2", "v2-NEW").equals("v2");
      assertInCacheNotInStore("k2", "v2-NEW");

      cache.evict("k2");
      assertInStoreNotInCache("k2", "v2-NEW");
      assert cache.replace("k2", "v2-REPLACED").equals("v2-NEW");
      assertInCacheNotInStore("k2", "v2-REPLACED");

      cache.evict("k2");
      assertInStoreNotInCache("k2", "v2-REPLACED");

      assert !cache.replace("k2", "some-rubbish", "v2-SHOULDNT-STORE"); // but should activate
      assertInCacheNotInStore("k2", "v2-REPLACED");

      cache.evict("k2");
      assertInStoreNotInCache("k2", "v2-REPLACED");

      assert cache.replace("k2", "v2-REPLACED", "v2-REPLACED-AGAIN");
      assertInCacheNotInStore("k2", "v2-REPLACED-AGAIN");

      cache.evict("k2");
      assertInStoreNotInCache("k2", "v2-REPLACED-AGAIN");

      assert cache.putIfAbsent("k2", "should-not-appear").equals("v2-REPLACED-AGAIN");
      assertInCacheNotInStore("k2", "v2-REPLACED-AGAIN");

      assert cache.putIfAbsent("k1", "v1-if-absent") == null;
      assertInCacheNotInStore("k1", "v1-if-absent");
   }

   public void testTransactions() throws Exception {
      assertNotInCacheAndStore("k1", "k2");

      tm.begin();
      cache.put("k1", "v1");
      cache.put("k2", "v2", lifespan, MILLISECONDS);
      Transaction t = tm.suspend();

      assertNotInCacheAndStore("k1", "k2");

      tm.resume(t);
      tm.commit();

      assertInCacheNotInStore("k1", "v1");
      assertInCacheNotInStore("k2", "v2", lifespan);

      tm.begin();
      cache.clear();
      t = tm.suspend();

      assertInCacheNotInStore("k1", "v1");
      assertInCacheNotInStore("k2", "v2", lifespan);
      tm.resume(t);
      tm.commit();

      assertNotInCacheAndStore("k1", "k2");

      tm.begin();
      cache.put("k1", "v1");
      cache.put("k2", "v2", lifespan, MILLISECONDS);
      t = tm.suspend();

      assertNotInCacheAndStore("k1", "k2");

      tm.resume(t);
      tm.rollback();

      assertNotInCacheAndStore("k1", "k2");
      cache.put("k1", "v1");
      cache.put("k2", "v2", lifespan, MILLISECONDS);

      assertInCacheNotInStore("k1", "v1");
      assertInCacheNotInStore("k2", "v2", lifespan);

      cache.evict("k1");
      cache.evict("k2");

      assertInStoreNotInCache("k1", "v1");
      assertInStoreNotInCache("k2", "v2", lifespan);
   }

   public void testPutMap() throws PersistenceException {
      assertNotInCacheAndStore("k1", "k2", "k3");
      cache.put("k1", "v1");
      cache.put("k2", "v2");

      cache.evict("k2");

      assertInCacheNotInStore("k1", "v1");
      assertInStoreNotInCache("k2", "v2");

      Map m = new HashMap();
      m.put("k1", "v1-NEW");
      m.put("k2", "v2-NEW");
      m.put("k3", "v3-NEW");

      cache.putAll(m);

      assertInCacheNotInStore("k1", "v1-NEW");
      assertInCacheNotInStore("k2", "v2-NEW");
      assertInCacheNotInStore("k3", "v3-NEW");
   }
}