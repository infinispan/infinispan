package org.infinispan.jmx;

import static org.infinispan.test.TestingUtil.checkMBeanOperationParameterNaming;
import static org.infinispan.test.TestingUtil.getCacheObjectName;
import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.v;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.jmx.MBeanServerLookup;
import org.infinispan.commons.jmx.TestMBeanServerLookup;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.dummy.DummyInMemoryStore;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import io.reactivex.rxjava3.exceptions.Exceptions;

/**
 * Test functionality in {@link org.infinispan.interceptors.impl.CacheMgmtInterceptor}.
 *
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 */
@Test(groups = "functional", testName = "jmx.CacheMgmtInterceptorMBeanTest")
public class CacheMgmtInterceptorMBeanTest extends SingleCacheManagerTest {
   private ObjectName mgmtInterceptor;
   private AdvancedCache<?, ?> advanced;
   private DummyInMemoryStore loader;
   private static final String JMX_DOMAIN = CacheMgmtInterceptorMBeanTest.class.getSimpleName();
   private final MBeanServerLookup mBeanServerLookup = TestMBeanServerLookup.create();

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      GlobalConfigurationBuilder globalConfiguration = new GlobalConfigurationBuilder();
      globalConfiguration
            .jmx().enabled(true)
            .domain(JMX_DOMAIN)
            .mBeanServerLookup(mBeanServerLookup)
            .metrics().accurateSize(true);

      ConfigurationBuilder configuration = getDefaultStandaloneCacheConfig(false);
      configuration.memory().maxCount(1)
            .persistence()
            .passivation(true)
            .addStore(DummyInMemoryStoreConfigurationBuilder.class);

      configuration.statistics().enable();

      cacheManager = TestCacheManagerFactory.createCacheManager(globalConfiguration, configuration);

      cacheManager.defineConfiguration("test", configuration.build());
      cache = cacheManager.getCache("test");
      advanced = cache.getAdvancedCache();
      mgmtInterceptor = getCacheObjectName(JMX_DOMAIN, "test(local)", "Statistics");
      loader = TestingUtil.getFirstStore(cache);

      return cacheManager;
   }

   @AfterMethod
   public void resetStats() throws Exception {
      mBeanServerLookup.getMBeanServer().invoke(mgmtInterceptor, "resetStatistics", new Object[0], new String[0]);
   }

   public void testJmxOperationMetadata() throws Exception {
      checkMBeanOperationParameterNaming(mBeanServerLookup.getMBeanServer(), mgmtInterceptor);
   }

   public void testEviction(Method m) throws Exception {
      assertEvictions(0);
      assertNull(cache.get(k(m, "1")));
      cache.put(k(m, "1"), v(m, 1));
      //test explicit evict command
      cache.evict(k(m, "1"));
      assertTrue("the entry should have been evicted", loader.contains(k(m, "1")));
      assertEvictions(1);
      assertEquals(v(m, 1), cache.get(k(m, "1")));
      //test implicit eviction
      cache.put(k(m, "2"), v(m, 2));
      // Evictions of unrelated keys are non blocking now so it may not be updated immediately
      eventuallyAssertEvictions(2);
      assertTrue("the entry should have been evicted", loader.contains(k(m, "1")));
   }

   public void testGetKeyValue() throws Exception {
      assertMisses(0);
      assertHits(0);
      assertEquals(0, advanced.getStats().getHits());
      assertAttributeValue("HitRatio", 0);

      cache.put("key", "value");

      assertMisses(0);
      assertHits(0);
      assertAttributeValue("HitRatio", 0);

      assertEquals("value", cache.get("key"));
      assertMisses(0);
      assertHits(1);
      assertAttributeValue("HitRatio", 1);

      assertNull(cache.get("key_ne"));
      assertNull(cache.get("key_ne"));
      assertNull(cache.get("key_ne"));
      assertMisses(3);
      assertHits(1);
      assertAttributeValue("HitRatio", 0.25f);
   }

   public void testStores() throws Exception {
      assertEvictions(0);
      assertStores(0);
      cache.put("key", "value");
      assertStores(1);
      cache.put("key", "value");
      assertStores(2);

      assertCurrentNumberOfEntries(1);
      assertApproximateEntries(1);
      cache.evict("key");
      assertCurrentNumberOfEntriesInMemory(0);
      assertApproximateEntriesInMemory(0);
      assertCurrentNumberOfEntries(1);
      assertApproximateEntries(1);

      Map<String, String> toAdd = new HashMap<>();
      toAdd.put("key", "value");
      toAdd.put("key2", "value2");
      cache.putAll(toAdd);
      assertStores(4);
      TestingUtil.cleanUpDataContainerForCache(cache);
      assertCurrentNumberOfEntriesInMemory(1);
      assertApproximateEntriesInMemory(1);
      assertCurrentNumberOfEntries(2);
      assertApproximateEntries(3);

      resetStats();

      toAdd = new HashMap<>();
      toAdd.put("key3", "value3");
      toAdd.put("key4", "value4");
      cache.putAll(toAdd);
      assertStores(2);
      TestingUtil.cleanUpDataContainerForCache(cache);
      assertCurrentNumberOfEntriesInMemory(1);
      eventuallyAssertEvictions(2);
      assertCurrentNumberOfEntries(4);
   }

   public void testStoresPutForExternalRead() throws Exception {
      assertStores(0);
      cache.putForExternalRead("key", "value");
      assertStores(1);
      cache.putForExternalRead("key", "value");
      assertStores(1);
   }

   public void testStoresPutIfAbsent() throws Exception {
      assertStores(0);
      cache.putIfAbsent("voooo", "doooo");
      assertStores(1);
      cache.putIfAbsent("voooo", "no-doooo");
      assertStores(1);
   }

   public void testRemoves() throws Exception {
      assertStores(0);
      assertRemoveHits(0);
      assertRemoveMisses(0);
      cache.put("key", "value");
      cache.put("key2", "value2");
      cache.put("key3", "value3");
      assertStores(3);
      assertRemoveHits(0);
      assertRemoveMisses(0);

      cache.remove("key");
      cache.remove("key3");
      cache.remove("key4");
      assertRemoveHits(2);
      assertRemoveMisses(1);

      cache.remove("key2");
      assertRemoveHits(3);
      assertRemoveMisses(1);
   }

   public void testGetAll() throws Exception {
      MBeanServer server = mBeanServerLookup.getMBeanServer();

      assertEquals(0, advanced.getStats().getMisses());
      assertEquals(0, advanced.getStats().getHits());
      String hitRatioString = server.getAttribute(mgmtInterceptor, "HitRatio").toString();
      Float hitRatio = Float.parseFloat(hitRatioString);
      assertEquals(0f, hitRatio);

      cache.put("key", "value");

      assertEquals(0, advanced.getStats().getMisses());
      assertEquals(0, advanced.getStats().getHits());
      hitRatioString = server.getAttribute(mgmtInterceptor, "HitRatio").toString();
      hitRatio = Float.parseFloat(hitRatioString);
      assertEquals(0f, hitRatio);

      Set<String> keySet = new HashSet<>();
      keySet.add("key");
      keySet.add("key1");
      advanced.getAll(keySet);
      assertEquals(1, advanced.getStats().getMisses());
      assertEquals(1, advanced.getStats().getHits());
      hitRatioString = server.getAttribute(mgmtInterceptor, "HitRatio").toString();
      hitRatio = Float.parseFloat(hitRatioString);
      assertEquals(0.5f, hitRatio);
   }

   private void eventuallyAssertAttributeValue(String attrName, float expectedValue) {
      eventuallyEquals(expectedValue, () -> {
         try {
            String receivedVal = mBeanServerLookup.getMBeanServer().getAttribute(mgmtInterceptor, attrName).toString();
            return Float.parseFloat(receivedVal);
         } catch (Exception e) {
            throw Exceptions.propagate(e);
         }
      });
   }

   private void assertAttributeValue(String attrName, float expectedValue) throws Exception {
      String receivedVal = mBeanServerLookup.getMBeanServer().getAttribute(mgmtInterceptor, attrName).toString();
      assertEquals("expecting " + expectedValue + " for " + attrName + ", but received " + receivedVal, expectedValue, Float.parseFloat(receivedVal));
   }

   private void eventuallyAssertEvictions(long expectedValue) {
      eventuallyAssertAttributeValue("Evictions", expectedValue);
      assertEquals(expectedValue, advanced.getStats().getEvictions());
   }

   private void assertEvictions(long expectedValue) throws Exception {
      assertAttributeValue("Evictions", expectedValue);
      assertEquals(expectedValue, advanced.getStats().getEvictions());
   }

   private void assertMisses(long expectedValue) throws Exception {
      assertAttributeValue("Misses", expectedValue);
      assertEquals(expectedValue, advanced.getStats().getMisses());
   }

   private void assertHits(long expectedValue) throws Exception {
      assertAttributeValue("Hits", expectedValue);
      assertEquals(expectedValue, advanced.getStats().getHits());
   }

   private void assertStores(long expectedValue) throws Exception {
      assertAttributeValue("Stores", expectedValue);
      assertEquals(expectedValue, advanced.getStats().getStores());
   }

   private void assertRemoveHits(long expectedValue) throws Exception {
      assertAttributeValue("RemoveHits", expectedValue);
      assertEquals(expectedValue, advanced.getStats().getRemoveHits());
   }

   private void assertRemoveMisses(long expectedValue) throws Exception {
      assertAttributeValue("RemoveMisses", expectedValue);
      assertEquals(expectedValue, advanced.getStats().getRemoveMisses());
   }

   private void assertCurrentNumberOfEntries(int expectedValue) throws Exception {
      assertAttributeValue("NumberOfEntries", expectedValue);
      assertEquals(expectedValue, advanced.getStats().getCurrentNumberOfEntries());
   }

   private void assertCurrentNumberOfEntriesInMemory(int expectedValue) throws Exception {
      assertAttributeValue("NumberOfEntriesInMemory", expectedValue);
      assertEquals(expectedValue, advanced.getStats().getCurrentNumberOfEntriesInMemory());
   }

   private void assertApproximateEntries(int expectedValue) throws Exception {
      assertAttributeValue("ApproximateEntries", expectedValue);
      assertEquals(expectedValue, advanced.getStats().getApproximateEntries());
   }

   private void assertApproximateEntriesInMemory(int expectedValue) throws Exception {
      assertAttributeValue("ApproximateEntriesInMemory", expectedValue);
      assertEquals(expectedValue, advanced.getStats().getApproximateEntriesInMemory());
   }
}
