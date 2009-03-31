package org.horizon.jmx;

import org.horizon.config.CacheLoaderManagerConfig;
import org.horizon.config.Configuration;
import org.horizon.config.GlobalConfiguration;
import org.horizon.container.entries.InternalEntryFactory;
import org.horizon.loader.CacheLoaderConfig;
import org.horizon.loader.CacheLoaderManager;
import org.horizon.loader.CacheStore;
import org.horizon.loader.jdbc.TableManipulation;
import org.horizon.test.fwk.UnitTestDatabaseManager;
import org.horizon.loader.jdbc.binary.JdbcBinaryCacheStoreConfig;
import org.horizon.loader.jdbc.connectionfactory.ConnectionFactoryConfig;
import org.horizon.manager.CacheManager;
import org.horizon.test.SingleCacheManagerTest;
import org.horizon.test.TestingUtil;
import org.horizon.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.util.Collections;

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
      globalConfiguration.setJmxDomain("ActivationAndPassivationInterceptorMBeanTest");
      globalConfiguration.setExposeGlobalJmxStatistics(true);
      cacheManager = TestCacheManagerFactory.createCacheManager(globalConfiguration);

      ConnectionFactoryConfig connectionFactoryConfig = UnitTestDatabaseManager.getUniqueConnectionFactoryConfig();
      TableManipulation tableManipulation = UnitTestDatabaseManager.buildDefaultTableManipulation();
      JdbcBinaryCacheStoreConfig config = new JdbcBinaryCacheStoreConfig(connectionFactoryConfig, tableManipulation);

      CacheLoaderManagerConfig clManagerConfig = new CacheLoaderManagerConfig();
      clManagerConfig.setPassivation(false);
      clManagerConfig.setCacheLoaderConfigs(Collections.singletonList((CacheLoaderConfig) config));
      Configuration configuration = getDefaultClusteredConfig(Configuration.CacheMode.LOCAL);
      configuration.setExposeJmxStatistics(true);
      configuration.setCacheLoaderManagerConfig(clManagerConfig);

      cacheManager.defineCache("test", configuration);
      cache = cacheManager.getCache("test");
      loaderInterceptorObjName = new ObjectName("ActivationAndPassivationInterceptorMBeanTest:cache-name=test(local),jmx-resource=CacheLoaderInterceptor");
      storeInterceptorObjName = new ObjectName("ActivationAndPassivationInterceptorMBeanTest:cache-name=test(local),jmx-resource=CacheStoreInterceptor");

      threadMBeanServer = PerThreadMBeanServerLookup.getThreadMBeanServer();
      cacheStore = TestingUtil.extractComponent(cache, CacheLoaderManager.class).getCacheStore();
      return cacheManager;
   }

   @AfterMethod
   public void resetStats() throws Exception {
      threadMBeanServer.invoke(loaderInterceptorObjName, "resetStatistics", new Object[0], new String[0]);
      threadMBeanServer.invoke(storeInterceptorObjName, "resetStatistics", new Object[0], new String[0]);
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
