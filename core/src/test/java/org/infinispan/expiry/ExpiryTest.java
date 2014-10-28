package org.infinispan.expiry;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.manager.CacheContainer;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.infinispan.test.TestingUtil.*;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

@Test(groups = "functional", testName = "expiry.ExpiryTest")
public class ExpiryTest extends AbstractInfinispanTest {

   public static final int EXPIRATION_TIMEOUT = 3000;
   public static final int IDLE_TIMEOUT = 3000;
   public static final int EVICTION_CHECK_TIMEOUT = 2000;
   CacheContainer cm;

   @BeforeMethod
   public void setUp() {
      cm = TestCacheManagerFactory.createCacheManager(false);
   }

   @AfterMethod
   public void tearDown() {
      TestingUtil.killCacheManagers(cm);
      cm = null;
   }

   public void testLifespanExpiryInPut() throws InterruptedException {
      Cache<String, String> cache = cm.getCache();
      long lifespan = EXPIRATION_TIMEOUT;
      cache.put("k", "v", lifespan, MILLISECONDS);

      DataContainer dc = cache.getAdvancedCache().getDataContainer();
      InternalCacheEntry se = dc.get("k");
      assert se.getKey().equals("k");
      assert se.getValue().equals("v");
      assert se.getLifespan() == lifespan;
      assert se.getMaxIdle() == -1;
      assert !se.isExpired(TIME_SERVICE.wallClockTime());
      assert cache.get("k").equals("v");
      Thread.sleep(lifespan + 100);
      assert se.isExpired(TIME_SERVICE.wallClockTime());
      assert cache.get("k") == null;
   }

   public void testIdleExpiryInPut() throws InterruptedException {
      Cache<String, String> cache = cm.getCache();
      long idleTime = IDLE_TIMEOUT;
      cache.put("k", "v", -1, MILLISECONDS, idleTime, MILLISECONDS);

      DataContainer dc = cache.getAdvancedCache().getDataContainer();
      InternalCacheEntry se = dc.get("k");
      assert se.getKey().equals("k");
      assert se.getValue().equals("v");
      assert se.getLifespan() == -1;
      assert se.getMaxIdle() == idleTime;
      assert !se.isExpired(TIME_SERVICE.wallClockTime());
      assert cache.get("k").equals("v");
      Thread.sleep(idleTime + 100);
      assert se.isExpired(TIME_SERVICE.wallClockTime());
      assert cache.get("k") == null;
   }

   public void testLifespanExpiryInPutAll() throws InterruptedException {
      Cache<String, String> cache = cm.getCache();
      final long startTime = now();
      final long lifespan = EXPIRATION_TIMEOUT;
      Map<String, String> m = new HashMap();
      m.put("k1", "v");
      m.put("k2", "v");
      cache.putAll(m, lifespan, MILLISECONDS);
      while (true) {
         String v1 = cache.get("k1");
         String v2 = cache.get("k2");
         if (moreThanDurationElapsed(startTime, lifespan))
            break;
         assertEquals("v", v1);
         assertEquals("v", v2);
         Thread.sleep(100);
      }

      //make sure that in the next 30 secs data is removed
      while (!moreThanDurationElapsed(startTime, lifespan + EVICTION_CHECK_TIMEOUT)) {
         if (cache.get("k1") == null && cache.get("k2") == null) return;
      }
      assert cache.get("k1") == null;
      assert cache.get("k2") == null;
   }

   public void testIdleExpiryInPutAll() throws InterruptedException {
      Cache<String, String> cache = cm.getCache();
      final long idleTime = EXPIRATION_TIMEOUT;
      Map<String, String> m = new HashMap<String, String>();
      m.put("k1", "v");
      m.put("k2", "v");
      long start = now();
      cache.putAll(m, -1, MILLISECONDS, idleTime, MILLISECONDS);
      assert "v".equals(cache.get("k1")) || moreThanDurationElapsed(start, idleTime);
      assert "v".equals(cache.get("k2")) || moreThanDurationElapsed(start, idleTime);

      Thread.sleep(idleTime + 100);

      assert cache.get("k1") == null;
      assert cache.get("k2") == null;
   }

   public void testLifespanExpiryInPutIfAbsent() throws InterruptedException {
      Cache<String, String> cache = cm.getCache();
      final long startTime = now();
      final long lifespan = EXPIRATION_TIMEOUT;
      assert cache.putIfAbsent("k", "v", lifespan, MILLISECONDS) == null;
      long partial = lifespan / 10;
      // Sleep some time within the lifespan boundaries
      Thread.sleep(lifespan - partial);
      assert "v".equals(cache.get("k")) || moreThanDurationElapsed(startTime, lifespan);
      // Sleep some time that guarantees that it'll be over the lifespan
      Thread.sleep(partial * 2);
      assert cache.get("k") == null;

      //make sure that in the next 2 secs data is removed
      while (!moreThanDurationElapsed(startTime, lifespan + EVICTION_CHECK_TIMEOUT)) {
         if (cache.get("k") == null) break;
         Thread.sleep(50);
      }
      assert cache.get("k") == null;

      cache.put("k", "v");
      assert cache.putIfAbsent("k", "v", lifespan, MILLISECONDS) != null;
   }

   public void testIdleExpiryInPutIfAbsent() throws InterruptedException {
      Cache<String, String> cache = cm.getCache();
      long idleTime = EXPIRATION_TIMEOUT;
      long start = now();
      assert cache.putIfAbsent("k", "v", -1, MILLISECONDS, idleTime, MILLISECONDS) == null;
      assert "v".equals(cache.get("k")) || moreThanDurationElapsed(start, idleTime);

      Thread.sleep(idleTime + 100);

      assert cache.get("k") == null;

      cache.put("k", "v");
      assert cache.putIfAbsent("k", "v", -1, MILLISECONDS, idleTime, MILLISECONDS) != null;
   }

   public void testLifespanExpiryInReplace() throws InterruptedException {
      Cache<String, String> cache = cm.getCache();
      final long lifespan = EXPIRATION_TIMEOUT;
      assert cache.get("k") == null;
      assert cache.replace("k", "v", lifespan, MILLISECONDS) == null;
      assert cache.get("k") == null;
      cache.put("k", "v-old");
      assert cache.get("k").equals("v-old");
      long startTime = now();
      assert cache.replace("k", "v", lifespan, MILLISECONDS) != null;
      assert "v".equals(cache.get("k")) || moreThanDurationElapsed(startTime, lifespan);
      while (true) {
         String v = cache.get("k");
         if (moreThanDurationElapsed(startTime, lifespan))
            break;
         assertEquals("v", v);
         Thread.sleep(100);
      }

      //make sure that in the next 2 secs data is removed
      while (!moreThanDurationElapsed(startTime, lifespan + EVICTION_CHECK_TIMEOUT)) {
         if (cache.get("k") == null) break;
         Thread.sleep(100);
      }
      assert cache.get("k") == null;


      startTime = now();
      cache.put("k", "v");
      assert cache.replace("k", "v", "v2", lifespan, MILLISECONDS);
      while (true) {
         String v = cache.get("k");
         if (moreThanDurationElapsed(startTime, lifespan))
            break;
         assertEquals("v2", v);
         Thread.sleep(100);
      }

      //make sure that in the next 2 secs data is removed
      while (!moreThanDurationElapsed(startTime, lifespan + EVICTION_CHECK_TIMEOUT)) {
         if (cache.get("k") == null) break;
         Thread.sleep(50);
      }
      assert cache.get("k") == null;
   }

   public void testIdleExpiryInReplace() throws InterruptedException {
      Cache<String, String> cache = cm.getCache();
      long idleTime = EXPIRATION_TIMEOUT;
      assert cache.get("k") == null;
      assert cache.replace("k", "v", -1, MILLISECONDS, idleTime, MILLISECONDS) == null;
      assert cache.get("k") == null;
      cache.put("k", "v-old");
      assert cache.get("k").equals("v-old");
      long start = now();
      assert cache.replace("k", "v", -1, MILLISECONDS, idleTime, MILLISECONDS) != null;
      assert "v".equals(cache.get("k")) || moreThanDurationElapsed(start, idleTime);

      Thread.sleep(idleTime + 100);
      assert cache.get("k") == null;

      cache.put("k", "v");
      assert cache.replace("k", "v", "v2", -1, MILLISECONDS, idleTime, MILLISECONDS);

      Thread.sleep(idleTime + 100);
      assert cache.get("k") == null;
   }

   public void testEntrySetAfterExpiryInPut(Method m) throws Exception {
      doTestEntrySetAfterExpiryInPut(m, cm);
   }

   public void testEntrySetAfterExpiryInTransaction(Method m) throws Exception {
      CacheContainer cc = createTransactionalCacheContainer();
      try {
         doEntrySetAfterExpiryInTransaction(m, cc);
      } finally {
         cc.stop();
      }
   }

   public void testEntrySetAfterExpiryWithStore(Method m) throws Exception {
      String location = TestingUtil.tmpDirectory(ExpiryTest.class);
      CacheContainer cc = createCacheContainerWithStore(location);
      try {
         doTestEntrySetAfterExpiryInPut(m, cc);
      } finally {
         cc.stop();
         TestingUtil.recursiveFileRemove(location);
      }
   }

   private CacheContainer createTransactionalCacheContainer() {
      return TestCacheManagerFactory.createCacheManager(TestCacheManagerFactory.getDefaultCacheConfiguration(true));
   }

   private CacheContainer createCacheContainerWithStore(String location) {
      ConfigurationBuilder b = new ConfigurationBuilder();
      b.persistence().addSingleFileStore().location(location);
      return TestCacheManagerFactory.createCacheManager(b);
   }

   private void doTestEntrySetAfterExpiryInPut(Method m, CacheContainer cc) throws Exception {
      Cache<Integer, String> cache = cc.getCache();
      Set<Map.Entry<Integer, String>> entries;
      Map dataIn = new HashMap();
      dataIn.put(1, v(m, 1));
      dataIn.put(2, v(m, 2));
      Set entriesIn = dataIn.entrySet();

      final long startTime = now();
      final long lifespan = EXPIRATION_TIMEOUT;
      cache.putAll(dataIn, lifespan, TimeUnit.MILLISECONDS);

      while (true) {
         // cache.entrySet is backing so expired entries will be removed so we have to make a copy
         entries = new HashSet<>(cache.entrySet());
         if (moreThanDurationElapsed(startTime, lifespan))
            break;
         // If the time hasn't lapsed then the entries should be the same
         assertEquals(entriesIn, entries);
         Thread.sleep(100);
      }

      // Make sure that in the next 20 secs data is removed
      while (!moreThanDurationElapsed(startTime, lifespan + EVICTION_CHECK_TIMEOUT)) {
         entries = cache.entrySet();
         if (entries.size() == 0) break;
      }

      assertEquals(0, entries.size());
   }

   private void doEntrySetAfterExpiryInTransaction(Method m, CacheContainer cc) throws Exception {
      Cache<Integer, String> cache = cc.getCache();
      Set<Map.Entry<Integer, String>> entries;
      Map dataIn = new HashMap();
      dataIn.put(1, v(m, 1));
      dataIn.put(2, v(m, 2));

      final long startTime = now();
      final long lifespan = EXPIRATION_TIMEOUT;
      cache.putAll(dataIn, lifespan, TimeUnit.MILLISECONDS);

      cache.getAdvancedCache().getTransactionManager().begin();
      try {
         Map txDataIn = new HashMap();
         txDataIn.put(3, v(m, 3));
         Map allEntriesIn = new HashMap(dataIn);
         // Update expectations
         allEntriesIn.putAll(txDataIn);
         // Add an entry within tx
         cache.putAll(txDataIn);

         while (true) {
            entries = new HashSet<>(cache.entrySet());
            if (moreThanDurationElapsed(startTime, lifespan))
               break;
            assertEquals(allEntriesIn.entrySet(), entries);
            Thread.sleep(100);
         }

         // Make sure that in the next 20 secs data is removed
         while (!moreThanDurationElapsed(startTime, lifespan + EVICTION_CHECK_TIMEOUT)) {
            entries = cache.entrySet();
            if (entries.size() == 1) break;
         }
      } finally {
         cache.getAdvancedCache().getTransactionManager().commit();
      }

      assertEquals(1, entries.size());
   }

   public void testKeySetAfterExpiryInPut(Method m) throws Exception {
      Cache<Integer, String> cache = cm.getCache();
      Set<Integer> keys;
      Map dataIn = new HashMap();
      dataIn.put(1, v(m, 1));
      dataIn.put(2, v(m, 2));
      Set keysIn = dataIn.keySet();

      final long startTime = now();
      final long lifespan = EXPIRATION_TIMEOUT;
      cache.putAll(dataIn, lifespan, TimeUnit.MILLISECONDS);

      while (true) {
         keys = new HashSet<>(cache.keySet());
         if (moreThanDurationElapsed(startTime, lifespan))
            break;
         assertEquals(keysIn, keys);
         Thread.sleep(100);
      }

      // Make sure that in the next 20 secs data is removed
      while (!moreThanDurationElapsed(startTime, lifespan + EVICTION_CHECK_TIMEOUT)) {
         keys = cache.keySet();
         if (keys.size() == 0) break;
      }

      assertEquals(0, keys.size());
   }

   public void testKeySetAfterExpiryInTransaction(Method m) throws Exception {
      CacheContainer cc = createTransactionalCacheContainer();
      try {
         doKeySetAfterExpiryInTransaction(m, cc);
      } finally {
         cc.stop();
      }
   }

   private void doKeySetAfterExpiryInTransaction(Method m, CacheContainer cc) throws Exception {
      Cache<Integer, String> cache = cc.getCache();
      Set<Integer> keys;
      Map dataIn = new HashMap();
      dataIn.put(1, v(m, 1));
      dataIn.put(2, v(m, 2));

      final long startTime = now();
      final long lifespan = EXPIRATION_TIMEOUT;
      cache.putAll(dataIn, lifespan, TimeUnit.MILLISECONDS);

      cache.getAdvancedCache().getTransactionManager().begin();
      try {
         Map txDataIn = new HashMap();
         txDataIn.put(3, v(m, 3));
         Map allEntriesIn = new HashMap(dataIn);
         // Update expectations
         allEntriesIn.putAll(txDataIn);
         // Add an entry within tx
         cache.putAll(txDataIn);

         while (true) {
            keys = new HashSet<>(cache.keySet());
            if (moreThanDurationElapsed(startTime, lifespan))
               break;
            assertEquals(allEntriesIn.keySet(), keys);
            Thread.sleep(100);
         }

         // Make sure that in the next 20 secs data is removed
         while (!moreThanDurationElapsed(startTime, lifespan + EVICTION_CHECK_TIMEOUT)) {
            keys = cache.keySet();
            if (keys.size() == 1) break;
         }
      } finally {
         cache.getAdvancedCache().getTransactionManager().commit();
      }

      assertEquals(1, keys.size());
   }

   public void testValuesAfterExpiryInPut(Method m) throws Exception {
      Cache<Integer, String> cache = cm.getCache();
      // Values come as a Collection, but comparison of HashMap#Values is done
      // by reference equality, so wrap the collection around to set to make
      // testing easier, given that we know that there are dup values.
      Collection<String> values;
      Map<Integer, String> dataIn = new HashMap<>();
      dataIn.put(1, v(m, 1));
      dataIn.put(2, v(m, 2));
      Collection<String> valuesIn = new ArrayList<>(dataIn.values());

      final long startTime = now();
      final long lifespan = EXPIRATION_TIMEOUT;
      cache.putAll(dataIn, lifespan, TimeUnit.MILLISECONDS);

      while (true) {
         values = new ArrayList<>(cache.values());
         if (moreThanDurationElapsed(startTime, lifespan))
            break;
         assertEquals(valuesIn, values);
         Thread.sleep(100);
      }

      // Make sure that in the next 20 secs data is removed
      while (!moreThanDurationElapsed(startTime, lifespan + EVICTION_CHECK_TIMEOUT)) {
         values = cache.values();
         if (values.size() == 0) break;
      }

      assertEquals(0, values.size());
   }

   public void testValuesAfterExpiryInTransaction(Method m) throws Exception {
      CacheContainer cc = createTransactionalCacheContainer();
      try {
         doValuesAfterExpiryInTransaction(m, cc);
      } finally {
         cc.stop();
      }
   }

   public void testTransientEntrypUpdates() {
      Cache<Integer, String> cache = cm.getCache();
      cache.put(1, "boo", -1, TimeUnit.SECONDS, 10, TimeUnit.SECONDS);
      cache.put(1, "boo2");
      cache.put(1, "boo3");
   }

   private void doValuesAfterExpiryInTransaction(Method m, CacheContainer cc) throws Exception {
      Cache<Integer, String> cache = cc.getCache();
      // Values come as a Collection, but comparison of HashMap#Values is done
      // by reference equality, so wrap the collection around to set to make
      // testing easier, given that we know that there are dup values.
      Collection<String> values;
      Map<Integer, String> dataIn = new HashMap<>();
      dataIn.put(1, v(m, 1));
      dataIn.put(2, v(m, 2));

      final long startTime = now();
      final long lifespan = EXPIRATION_TIMEOUT;
      cache.putAll(dataIn, lifespan, TimeUnit.MILLISECONDS);

      cache.getAdvancedCache().getTransactionManager().begin();
      try {
         Map<Integer, String> txDataIn = new HashMap<>();
         txDataIn.put(3, v(m, 3));
         Collection<String> allValuesIn = new ArrayList<>(dataIn.values());
         allValuesIn.addAll(txDataIn.values());

         // Add an entry within tx
         cache.putAll(txDataIn);

         while (true) {
            values = new ArrayList<>(cache.values());
            if (moreThanDurationElapsed(startTime, lifespan))
               break;
            assertTrue(allValuesIn.containsAll(values));
            Thread.sleep(100);
         }

         // Make sure that in the next 20 secs data is removed
         while (!moreThanDurationElapsed(startTime, lifespan + EVICTION_CHECK_TIMEOUT)) {
            values = cache.values();
            if (values.size() == 1) break;
         }
      } finally {
         cache.getAdvancedCache().getTransactionManager().commit();
      }

      assertEquals(1, values.size());
   }
}
