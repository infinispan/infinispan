package org.infinispan.jmx;

import org.infinispan.config.CacheLoaderManagerConfig;
import org.infinispan.config.Configuration;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.container.entries.InternalEntryFactory;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.loaders.CacheStore;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStore;
import org.infinispan.manager.CacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import javax.management.MBeanServer;
import javax.management.ObjectName;

/**
 * Tests the jmx functionality from CacheLoaderInterceptor and CacheStoreInterceptor.
 *
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "jmx.CacheLoaderAndCacheStoreInterceptorMBeanTest")
public class CacheLoaderAndCacheStoreInterceptorMBeanTest extends SingleCacheManagerTest {
   private ObjectName loaderInterceptorObjName;
   private ObjectName storeInterceptorObjName;
   private MBeanServer threadMBeanServer;
   private CacheStore cacheStore;

   protected CacheManager createCacheManager() throws Exception {
      GlobalConfiguration globalConfiguration = GlobalConfiguration.getNonClusteredDefault();
      globalConfiguration.setMBeanServerLookup(PerThreadMBeanServerLookup.class.getName());
      globalConfiguration.setJmxDomain("CacheLoaderAndCacheStoreInterceptorMBeanTest");
      globalConfiguration.setExposeGlobalJmxStatistics(true);
      cacheManager = TestCacheManagerFactory.createCacheManager(globalConfiguration);

      DummyInMemoryCacheStore.Cfg cfg = new DummyInMemoryCacheStore.Cfg();

      CacheLoaderManagerConfig clManagerConfig = new CacheLoaderManagerConfig();
      clManagerConfig.setPassivation(false);
      clManagerConfig.addCacheLoaderConfig(cfg);
      Configuration configuration = getDefaultClusteredConfig(Configuration.CacheMode.LOCAL);
      configuration.setExposeJmxStatistics(true);
      configuration.setCacheLoaderManagerConfig(clManagerConfig);

      cacheManager.defineConfiguration("test", configuration);
      cache = cacheManager.getCache("test");
      loaderInterceptorObjName = new ObjectName("CacheLoaderAndCacheStoreInterceptorMBeanTest:cache-name=test(local),jmx-resource=CacheLoader");
      storeInterceptorObjName = new ObjectName("CacheLoaderAndCacheStoreInterceptorMBeanTest:cache-name=test(local),jmx-resource=CacheStore");

      threadMBeanServer = PerThreadMBeanServerLookup.getThreadMBeanServer();
      cacheStore = TestingUtil.extractComponent(cache, CacheLoaderManager.class).getCacheStore();
      return cacheManager;
   }

   @AfterMethod
   public void resetStats() throws Exception {
      threadMBeanServer.invoke(loaderInterceptorObjName, "resetStatistics", new Object[0], new String[0]);
      threadMBeanServer.invoke(storeInterceptorObjName, "resetStatistics", new Object[0], new String[0]);
   }
   
   @AfterClass(alwaysRun=true)
   protected void destroyAfterClass() {      
      super.destroyAfterClass();
      cacheStore = null; threadMBeanServer =null;storeInterceptorObjName=null;
      loaderInterceptorObjName =null;
   }


   public void testPutKeyValue() throws Exception {
      assertStoreAccess(0, 0, 0);
      cache.put("key", "value");
      assertStoreAccess(0, 0, 1);
      cache.put("key", "value2");
      assertStoreAccess(0, 0, 2);

      cacheStore.store(InternalEntryFactory.create("a", "b"));
      cache.put("a", "c");
      assertStoreAccess(1, 0, 3);
      assert cacheStore.load("a").getValue().equals("c");
   }

   public void testGetValue() throws Exception {
      assertStoreAccess(0, 0, 0);
      cache.put("key", "value");
      assertStoreAccess(0, 0, 1);

      assert cache.get("key").equals("value");
      assertStoreAccess(0, 0, 1);

      cacheStore.store(InternalEntryFactory.create("a", "b"));
      assert cache.get("a").equals("b");
      assertStoreAccess(1, 0, 1);

      assert cache.get("no_such_key") == null;
      assertStoreAccess(1, 1, 1);
   }

   public void testRemoveValue() throws Exception {
      assertStoreAccess(0, 0, 0);
      cache.put("key", "value");
      assertStoreAccess(0, 0, 1);

      assert cache.get("key").equals("value");
      assertStoreAccess(0, 0, 1);

      assert cache.remove("key").equals("value");
      assertStoreAccess(0, 0, 1);

      cache.remove("no_such_key");
      assertStoreAccess(0, 1, 1);

      cacheStore.store(InternalEntryFactory.create("a", "b"));
      assert cache.remove("a").equals("b");
      assertStoreAccess(1, 1, 1);
   }

   public void testReplaceCommand() throws Exception {
      assertStoreAccess(0, 0, 0);
      cache.put("key", "value");
      assertStoreAccess(0, 0, 1);

      assert cache.replace("key", "value2").equals("value");
      assertStoreAccess(0, 0, 2);

      cacheStore.store(InternalEntryFactory.create("a", "b"));
      assert cache.replace("a", "c").equals("b");
      assertStoreAccess(1, 0, 3);

      assert cache.replace("no_such_key", "c") == null;
      assertStoreAccess(1, 1, 3);
   }

   private void assertStoreAccess(int loadsCount, int missesCount, int storeCount) throws Exception {
      assertLoadCount(loadsCount, missesCount);
      assertStoreCount(storeCount);
   }

   private void assertLoadCount(int loadsCount, int missesCount) throws Exception {
      Object actualLoadCount = threadMBeanServer.getAttribute(loaderInterceptorObjName, "CacheLoaderLoads");
      assert Integer.valueOf(actualLoadCount.toString()).equals(loadsCount) : "expected " + loadsCount + " loads count and received " + actualLoadCount;
      Object actualMissesCount = threadMBeanServer.getAttribute(loaderInterceptorObjName, "CacheLoaderMisses");
      assert Integer.valueOf(actualMissesCount.toString()).equals(missesCount) : "expected " + missesCount + " misses count, and received " + actualMissesCount;
   }

   private void assertStoreCount(int count) throws Exception {
      Object actualStoreCount = threadMBeanServer.getAttribute(storeInterceptorObjName, "CacheLoaderStores");
      assert Integer.valueOf(actualStoreCount.toString()).equals(count) : "expected " + count + " store counts, but received " + actualStoreCount;
   }
}
