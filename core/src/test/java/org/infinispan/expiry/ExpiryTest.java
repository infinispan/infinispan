package org.infinispan.expiry;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.infinispan.test.TestingUtil.v;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.time.ControlledTimeService;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.manager.CacheContainer;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "expiry.ExpiryTest")
public class ExpiryTest extends AbstractInfinispanTest {

   public static final int EXPIRATION_TIMEOUT = 3000;
   public static final int IDLE_TIMEOUT = 3000;
   CacheContainer cm;

   protected ControlledTimeService timeService;

   @BeforeMethod
   public void setUp() {
      cm = TestCacheManagerFactory.createCacheManager(false);
      timeService = new ControlledTimeService();
      TestingUtil.replaceComponent(cm, TimeService.class, timeService, true);
   }

   @AfterMethod
   public void tearDown() {
      TestingUtil.killCacheManagers(cm);
      cm = null;
   }

   public void testLifespanExpiryInPut() throws InterruptedException {
      Cache<String, String> cache = getCache();
      long lifespan = EXPIRATION_TIMEOUT;
      cache.put("k", "v", lifespan, MILLISECONDS);

      DataContainer dc = cache.getAdvancedCache().getDataContainer();
      InternalCacheEntry se = dc.get("k");
      assert se.getKey().equals("k");
      assert se.getValue().equals("v");
      assert se.getLifespan() == lifespan;
      assert se.getMaxIdle() == -1;
      assert !se.isExpired(timeService.wallClockTime());
      assert cache.get("k").equals("v");
      timeService.advance(lifespan + 100);
      assert se.isExpired(timeService.wallClockTime());
      assert cache.get("k") == null;
   }

   protected Cache<String, String> getCache() {
      return cm.getCache();
   }

   public void testIdleExpiryInPut() throws InterruptedException {
      Cache<String, String> cache = getCache();
      long idleTime = IDLE_TIMEOUT;
      cache.put("k", "v", -1, MILLISECONDS, idleTime, MILLISECONDS);

      DataContainer dc = cache.getAdvancedCache().getDataContainer();
      InternalCacheEntry se = dc.get("k");
      assert se.getKey().equals("k");
      assert se.getValue().equals("v");
      assert se.getLifespan() == -1;
      assert se.getMaxIdle() == idleTime;
      assert !se.isExpired(timeService.wallClockTime());
      assert cache.get("k").equals("v");
      timeService.advance(idleTime + 100);
      assertTrue(se.isExpired(timeService.wallClockTime()));
      assertNull(cache.get("k"));
   }

   public void testLifespanExpiryInPutAll() throws InterruptedException {
      Cache<String, String> cache = getCache();
      final long lifespan = EXPIRATION_TIMEOUT;
      Map<String, String> m = new HashMap();
      m.put("k1", "v");
      m.put("k2", "v");
      cache.putAll(m, lifespan, MILLISECONDS);
      String v1 = cache.get("k1");
      String v2 = cache.get("k2");
      assertEquals("v", v1);
      assertEquals("v", v2);
      timeService.advance(lifespan + 100);

      assertNull(cache.get("k1"));
      assertNull(cache.get("k2"));
   }

   public void testIdleExpiryInPutAll() throws InterruptedException {
      Cache<String, String> cache = getCache();
      final long idleTime = EXPIRATION_TIMEOUT;
      Map<String, String> m = new HashMap<String, String>();
      m.put("k1", "v");
      m.put("k2", "v");
      cache.putAll(m, -1, MILLISECONDS, idleTime, MILLISECONDS);
      assertEquals("v", cache.get("k1"));
      assertEquals("v", cache.get("k2"));

      timeService.advance(idleTime + 100);

      assertNull(cache.get("k1"));
      assertNull(cache.get("k2"));
   }

   public void testLifespanExpiryInPutIfAbsent() throws InterruptedException {
      Cache<String, String> cache = getCache();
      final long lifespan = EXPIRATION_TIMEOUT;
      assert cache.putIfAbsent("k", "v", lifespan, MILLISECONDS) == null;
      long partial = lifespan / 10;
      // Sleep some time within the lifespan boundaries
      timeService.advance(lifespan - partial);
      assertEquals("v", cache.get("k"));
      // Sleep some time that guarantees that it'll be over the lifespan
      timeService.advance(lifespan);
      assertNull(cache.get("k"));

      cache.put("k", "v");
      assert cache.putIfAbsent("k", "v", lifespan, MILLISECONDS) != null;
   }

   public void testIdleExpiryInPutIfAbsent() throws InterruptedException {
      Cache<String, String> cache = getCache();
      long idleTime = EXPIRATION_TIMEOUT;
      assertNull(cache.putIfAbsent("k", "v", -1, MILLISECONDS, idleTime, MILLISECONDS));
      assertEquals("v", cache.get("k"));

      timeService.advance(idleTime + 100);

      assertNull(cache.get("k"));

      cache.put("k", "v");
      assertNotNull(cache.putIfAbsent("k", "v", -1, MILLISECONDS, idleTime, MILLISECONDS));
   }

   public void testLifespanExpiryInReplace() throws InterruptedException {
      Cache<String, String> cache = getCache();
      final long lifespan = EXPIRATION_TIMEOUT;
      assertNull(cache.get("k"));
      assertNull(cache.replace("k", "v", lifespan, MILLISECONDS));
      assertNull(cache.get("k"));
      cache.put("k", "v-old");
      assertEquals("v-old", cache.get("k"));
      assertNotNull(cache.replace("k", "v", lifespan, MILLISECONDS));
      assertEquals("v", cache.get("k"));

      timeService.advance(lifespan + 100);

      assertNull(cache.get("k"));


      cache.put("k", "v");
      assertTrue(cache.replace("k", "v", "v2", lifespan, MILLISECONDS));

      timeService.advance(lifespan + 100);

      assert cache.get("k") == null;
   }

   public void testIdleExpiryInReplace() throws InterruptedException {
      Cache<String, String> cache = getCache();
      long idleTime = EXPIRATION_TIMEOUT;
      assertNull(cache.get("k"));
      assertNull(cache.replace("k", "v", -1, MILLISECONDS, idleTime, MILLISECONDS));
      assertNull(cache.get("k"));
      cache.put("k", "v-old");
      assertEquals("v-old", cache.get("k"));
      assertNotNull(cache.replace("k", "v", -1, MILLISECONDS, idleTime, MILLISECONDS));
      assertEquals("v", cache.get("k"));

      timeService.advance(idleTime + 100);
      assertNull(cache.get("k"));

      cache.put("k", "v");
      assertTrue(cache.replace("k", "v", "v2", -1, MILLISECONDS, idleTime, MILLISECONDS));

      timeService.advance(idleTime + 100);
      assertNull(cache.get("k"));
   }

   @Test
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
      String location = CommonsTestingUtil.tmpDirectory(ExpiryTest.class);
      CacheContainer cc = createCacheContainerWithStore(location);
      try {
         doTestEntrySetAfterExpiryInPut(m, cc);
      } finally {
         cc.stop();
         Util.recursiveFileRemove(location);
      }
   }

   private CacheContainer createTransactionalCacheContainer() {
      CacheContainer cc = TestCacheManagerFactory.createCacheManager(
              TestCacheManagerFactory.getDefaultCacheConfiguration(true));
      TestingUtil.replaceComponent(cc, TimeService.class, timeService, true);
      return cc;
   }

   private CacheContainer createCacheContainerWithStore(String location) {
      GlobalConfigurationBuilder globalBuilder = new GlobalConfigurationBuilder().nonClusteredDefault();
      globalBuilder.globalState().persistentLocation(location);
      CacheContainer cc = TestCacheManagerFactory.createCacheManager(globalBuilder, new ConfigurationBuilder());
      TestingUtil.replaceComponent(cc, TimeService.class, timeService, true);
      return cc;
   }

   private void doTestEntrySetAfterExpiryInPut(Method m, CacheContainer cc) throws Exception {
      Cache<Integer, String> cache = cc.getCache();
      Map dataIn = new HashMap();
      dataIn.put(1, v(m, 1));
      dataIn.put(2, v(m, 2));

      final long lifespan = EXPIRATION_TIMEOUT;
      cache.putAll(dataIn, lifespan, TimeUnit.MILLISECONDS);

      timeService.advance(lifespan + 100);

      assertEquals(0, cache.entrySet().size());
   }

   private void doEntrySetAfterExpiryInTransaction(Method m, CacheContainer cc) throws Exception {
      Cache<Integer, String> cache = cc.getCache();
      Set<Map.Entry<Integer, String>> entries;
      Map dataIn = new HashMap();
      dataIn.put(1, v(m, 1));
      dataIn.put(2, v(m, 2));

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

         timeService.advance(lifespan + 100);
      } finally {
         cache.getAdvancedCache().getTransactionManager().commit();
      }

      assertEquals(1, cache.entrySet().size());
   }

   public void testKeySetAfterExpiryInPut(Method m) throws Exception {
      Cache<Integer, String> cache = cm.getCache();
      Map dataIn = new HashMap();
      dataIn.put(1, v(m, 1));
      dataIn.put(2, v(m, 2));

      final long lifespan = EXPIRATION_TIMEOUT;
      cache.putAll(dataIn, lifespan, TimeUnit.MILLISECONDS);

      timeService.advance(lifespan + 100);

      assertEquals(0, cache.keySet().size());
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
      Map dataIn = new HashMap();
      dataIn.put(1, v(m, 1));
      dataIn.put(2, v(m, 2));

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

         timeService.advance(lifespan + 100);
      } finally {
         cache.getAdvancedCache().getTransactionManager().commit();
      }

      assertEquals(1, cache.keySet().size());
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

      final long lifespan = EXPIRATION_TIMEOUT;
      cache.putAll(dataIn, lifespan, TimeUnit.MILLISECONDS);

      timeService.advance(lifespan + 100);

      assertEquals(0, cache.values().size());
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
      Map<Integer, String> dataIn = new HashMap<>();
      dataIn.put(1, v(m, 1));
      dataIn.put(2, v(m, 2));

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

         timeService.advance(lifespan + 100);
      } finally {
         cache.getAdvancedCache().getTransactionManager().commit();
      }

      assertEquals(1, cache.values().size());
   }
}
