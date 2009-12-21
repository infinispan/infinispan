package org.infinispan.jmx;

import org.infinispan.AdvancedCache;
import org.infinispan.config.Configuration;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.manager.CacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.util.HashMap;
import java.util.Map;

/**
 * Test functionality in {@link org.infinispan.interceptors.CacheMgmtInterceptor}.
 *
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "jmx.CacheMgmtInterceptorMBeanTest")
public class CacheMgmtInterceptorMBeanTest extends SingleCacheManagerTest {
   private ObjectName mgmtInterceptor;
   private MBeanServer threadMBeanServer;
   AdvancedCache advanced;

   protected CacheManager createCacheManager() throws Exception {
      GlobalConfiguration globalConfiguration = GlobalConfiguration.getNonClusteredDefault();
      globalConfiguration.setExposeGlobalJmxStatistics(true);
      globalConfiguration.setMBeanServerLookup(PerThreadMBeanServerLookup.class.getName());
      globalConfiguration.setJmxDomain("CacheMgmtInterceptorMBeanTest");
      cacheManager = TestCacheManagerFactory.createCacheManager(globalConfiguration);

      Configuration configuration = getDefaultClusteredConfig(Configuration.CacheMode.LOCAL);
      configuration.setExposeJmxStatistics(true);
      cacheManager.defineConfiguration("test", configuration);
      cache = cacheManager.getCache("test");
      advanced = cache.getAdvancedCache();
      mgmtInterceptor = new ObjectName("CacheMgmtInterceptorMBeanTest:cache-name=test(local),jmx-resource=Statistics");

      threadMBeanServer = PerThreadMBeanServerLookup.getThreadMBeanServer();
      return cacheManager;
   }

   @AfterMethod
   public void resetStats() throws Exception {
      threadMBeanServer.invoke(mgmtInterceptor, "resetStatistics", new Object[0], new String[0]);
   }

   public void testEviction() throws Exception {
      assertEvictions(0);
      cache.put("key", "value");
      assertEvictions(0);
      cache.evict("key");
      assertEvictions(1);
      cache.evict("does_not_exist");
      assertEvictions(2);
   }

   public void testGetKeyValue() throws Exception {
      assertMisses(0);
      assertHits(0);
      assert 0 == advanced.getStats().getHits();
      assertAttributeValue("HitRatio", 0);

      cache.put("key", "value");

      assertMisses(0);
      assertHits(0);
      assertAttributeValue("HitRatio", 0);

      assert cache.get("key").equals("value");
      assertMisses(0);
      assertHits(1);
      assertAttributeValue("HitRatio", 1);

      assert cache.get("key_ne") == null;
      assert cache.get("key_ne") == null;
      assert cache.get("key_ne") == null;
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

      Map toAdd = new HashMap();
      toAdd.put("key", "value");
      toAdd.put("key2", "value2");
      cache.putAll(toAdd);
      assertStores(4);
      assertCurrentNumberOfEntries(2);

      resetStats();

      toAdd = new HashMap();
      toAdd.put("key3", "value3");
      toAdd.put("key4", "value4");
      cache.putAll(toAdd);
      assertStores(2);
      assertCurrentNumberOfEntries(4);
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

   private void assertAttributeValue(String attrName, float expectedValue) throws Exception {
      String receivedVal = threadMBeanServer.getAttribute(mgmtInterceptor, attrName).toString();
      assert Float.parseFloat(receivedVal) == expectedValue : "expecting " + expectedValue + " for " + attrName + ", but received " + receivedVal;
   }

   private void assertEvictions(float expectedValue) throws Exception {
      assertAttributeValue("Evictions", expectedValue);
      assert expectedValue == advanced.getStats().getEvictions();
   }

   private void assertMisses(float expectedValue) throws Exception {
      assertAttributeValue("Misses", expectedValue);
      assert expectedValue == advanced.getStats().getMisses();
   }

   private void assertHits(float expectedValue) throws Exception {
      assertAttributeValue("Hits", expectedValue);
      assert expectedValue == advanced.getStats().getHits();
   }

   private void assertStores(float expectedValue) throws Exception {
      assertAttributeValue("Stores", expectedValue);
      assert expectedValue == advanced.getStats().getStores();
   }

   private void assertRemoveHits(float expectedValue) throws Exception {
      assertAttributeValue("RemoveHits", expectedValue);
      assert expectedValue == advanced.getStats().getRemoveHits();
   }

   private void assertRemoveMisses(float expectedValue) throws Exception {
      assertAttributeValue("RemoveMisses", expectedValue);
      assert expectedValue == advanced.getStats().getRemoveMisses();
   }

   private void assertCurrentNumberOfEntries(float expectedValue) throws Exception {
      assertAttributeValue("NumberOfEntries", expectedValue);
      assert expectedValue == advanced.getStats().getCurrentNumberOfEntries();
   }

}
