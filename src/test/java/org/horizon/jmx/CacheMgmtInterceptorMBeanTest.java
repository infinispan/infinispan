package org.horizon.jmx;

import org.horizon.config.Configuration;
import org.horizon.config.GlobalConfiguration;
import org.horizon.manager.CacheManager;
import org.horizon.manager.DefaultCacheManager;
import org.horizon.test.SingleCacheManagerTest;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.util.Map;
import java.util.HashMap;

/**
 * Test functionality in {@link org.horizon.interceptors.CacheMgmtInterceptor}.
 *
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "jmx.CacheMgmtInterceptorMBeanTest", testName = "jmx.CacheMgmtInterceptorMBeanTest")
public class CacheMgmtInterceptorMBeanTest extends SingleCacheManagerTest {
   private ObjectName mgmtInterceptor;
   private MBeanServer threadMBeanServer;

   protected CacheManager createCacheManager() throws Exception {
      GlobalConfiguration globalConfiguration = GlobalConfiguration.getNonClusteredDefault();
      globalConfiguration.setMBeanServerLookup(PerThreadMBeanServerLookup.class.getName());
      globalConfiguration.setJmxDomain("CacheMgmtInterceptorMBeanTest");
      cacheManager = new DefaultCacheManager(globalConfiguration);

      Configuration configuration = getDefaultClusteredConfig(Configuration.CacheMode.LOCAL);
      cacheManager.defineCache("test", configuration);
      cache = cacheManager.getCache("test");
      mgmtInterceptor = new ObjectName("CacheMgmtInterceptorMBeanTest:cache-name=test(local),jmx-resource=CacheMgmtInterceptor");

      threadMBeanServer = PerThreadMBeanServerLookup.getThreadMBeanServer();
      return cacheManager;
   }

   @AfterMethod
   public void resetStats() throws Exception {
      threadMBeanServer.invoke(mgmtInterceptor, "resetStatistics", new Object[0], new String[0]);
   }

   public void testEviction() throws Exception {
      assertAttributeValue("Evictions", 0);
      cache.put("key", "value");
      assertAttributeValue("Evictions", 0);
      cache.evict("key");
      assertAttributeValue("Evictions", 1);
      cache.evict("does_not_exist");
      assertAttributeValue("Evictions", 2);
   }

   public void testGetKeyValue() throws Exception {
      assertAttributeValue("Misses", 0);
      assertAttributeValue("Hits", 0);
      assertAttributeValue("HitRatio", 0);

      cache.put("key", "value");

      assertAttributeValue("Misses", 0);
      assertAttributeValue("Hits", 0);
      assertAttributeValue("HitRatio", 0);

      assert cache.get("key").equals("value");
      assertAttributeValue("Misses", 0);
      assertAttributeValue("Hits", 1);
      assertAttributeValue("HitRatio", 1);

      assert cache.get("key_ne") == null;
      assert cache.get("key_ne") == null;
      assert cache.get("key_ne") == null;
      assertAttributeValue("Misses", 3);
      assertAttributeValue("Hits", 1);
      assertAttributeValue("HitRatio", 0.25f);
   }

   public void testStores() throws Exception {
      assertAttributeValue("Evictions", 0);
      assertAttributeValue("Stores", 0);
      cache.put("key", "value");
      assertAttributeValue("Stores", 1);
      cache.put("key", "value");
      assertAttributeValue("Stores", 2);

      Map toAdd = new HashMap();
      toAdd.put("key", "value");
      toAdd.put("key2", "value2");
      cache.putAll(toAdd);
      assertAttributeValue("Stores", 4);
   }

   private void assertAttributeValue(String attrName, float expectedValue) throws Exception {
      String receivedVal = threadMBeanServer.getAttribute(mgmtInterceptor, attrName).toString();
      assert Float.parseFloat(receivedVal) == expectedValue : "expecting " + expectedValue + " for " + attrName + ", but received " + receivedVal;
   }
}
