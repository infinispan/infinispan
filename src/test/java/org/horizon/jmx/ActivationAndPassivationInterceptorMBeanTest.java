package org.horizon.jmx;

import org.horizon.Cache;
import org.horizon.config.CacheLoaderManagerConfig;
import org.horizon.config.Configuration;
import org.horizon.config.GlobalConfiguration;
import org.horizon.container.entries.InternalEntryFactory;
import org.horizon.loader.CacheLoaderConfig;
import org.horizon.loader.CacheLoaderManager;
import org.horizon.loader.CacheStore;
import org.horizon.loader.jdbc.TableManipulation;
import org.horizon.loader.jdbc.UnitTestDatabaseManager;
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
import java.util.HashMap;
import java.util.Map;

/**
 * Tester class for ActivationInterceptor and PassivationInterceptor.
 *
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "jmx.ActivationAndPassivationInterceptorMBeanTest")
public class ActivationAndPassivationInterceptorMBeanTest extends SingleCacheManagerTest {

   Cache cache;
   MBeanServer threadMBeanServer;
   ObjectName activationInterceptorObjName;
   ObjectName passivationInterceptorObjName;
   CacheStore cacheStore;

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
      clManagerConfig.setPassivation(true);
      clManagerConfig.setCacheLoaderConfigs(Collections.singletonList((CacheLoaderConfig) config));
      Configuration configuration = getDefaultClusteredConfig(Configuration.CacheMode.LOCAL);
      configuration.setExposeJmxStatistics(true);
      configuration.setCacheLoaderManagerConfig(clManagerConfig);

      cacheManager.defineCache("test", configuration);
      cache = cacheManager.getCache("test");
      activationInterceptorObjName = new ObjectName("ActivationAndPassivationInterceptorMBeanTest:cache-name=test(local),jmx-resource=ActivationInterceptor");
      passivationInterceptorObjName = new ObjectName("ActivationAndPassivationInterceptorMBeanTest:cache-name=test(local),jmx-resource=PassivationInterceptor");

      threadMBeanServer = PerThreadMBeanServerLookup.getThreadMBeanServer();
      cacheStore = TestingUtil.extractComponent(cache, CacheLoaderManager.class).getCacheStore();
      return cacheManager;
   }

   @AfterMethod
   public void resetStats() throws Exception {
      threadMBeanServer.invoke(activationInterceptorObjName, "resetStatistics", new Object[0], new String[0]);
   }

   public void testDisbaleStatistics() throws Exception {
      threadMBeanServer.invoke(activationInterceptorObjName, "setStatisticsEnabled", new Object[]{Boolean.FALSE}, new String[]{"boolean"});
      assert threadMBeanServer.getAttribute(activationInterceptorObjName, "Activations").toString().equals("N/A");
      threadMBeanServer.invoke(activationInterceptorObjName, "setStatisticsEnabled", new Object[]{Boolean.TRUE}, new String[]{"boolean"});
   }

   public void testActivationOnGet() throws Exception {
      assertActivationCount(0);
      assert cache.get("key") == null;
      assertActivationCount(0);
      cacheStore.store(InternalEntryFactory.create("key", "value"));
      assert cacheStore.containsKey("key");
      assert cache.get("key").equals("value");
      assertActivationCount(1);
      assert !cacheStore.containsKey("key");
   }

   public void testActivationOnPut() throws Exception {
      assertActivationCount(0);
      assert cache.get("key") == null;
      assertActivationCount(0);
      cacheStore.store(InternalEntryFactory.create("key", "value"));
      assert cacheStore.containsKey("key");
      cache.put("key", "value2");
      assert cache.get("key").equals("value2");
      assertActivationCount(1);
      assert !cacheStore.containsKey("key") : "this should only be persisted on evict";
   }

   public void testActivationOnRemove() throws Exception {
      assertActivationCount(0);
      assert cache.get("key") == null;
      assertActivationCount(0);
      cacheStore.store(InternalEntryFactory.create("key", "value"));
      assert cacheStore.containsKey("key");
      assert cache.remove("key").equals("value");
      assertActivationCount(1);
      assert !cacheStore.containsKey("key");
   }

   public void testActivationOnReplace() throws Exception {
      assertActivationCount(0);
      assert cache.get("key") == null;
      assertActivationCount(0);
      cacheStore.store(InternalEntryFactory.create("key", "value"));
      assert cacheStore.containsKey("key");
      assert cache.replace("key", "value2").equals("value");
      assertActivationCount(1);
      assert !cacheStore.containsKey("key");
   }

   public void testActivationOnPutMap() throws Exception {
      assertActivationCount(0);
      assert cache.get("key") == null;
      assertActivationCount(0);
      cacheStore.store(InternalEntryFactory.create("key", "value"));
      assert cacheStore.containsKey("key");

      Map toAdd = new HashMap();
      toAdd.put("key", "value2");
      cache.putAll(toAdd);
      assertActivationCount(1);
      assert cache.get("key").equals("value2");
      assert !cacheStore.containsKey("key");
   }

   public void testPassivationOnEvict() throws Exception {
      assertPassivationCount(0);
      cache.put("key", "val");
      cache.put("key2", "val2");
      cache.evict("key");
      assertPassivationCount(1);
      cache.evict("key2");
      assertPassivationCount(2);
      cache.evict("not_existing_key");
      assertPassivationCount(2);
   }

   private void assertActivationCount(int activationCount) throws Exception {
      assert Integer.valueOf(threadMBeanServer.getAttribute(activationInterceptorObjName, "Activations").toString()).equals(activationCount);
   }

   private void assertPassivationCount(int activationCount) throws Exception {
      assert Integer.valueOf(threadMBeanServer.getAttribute(passivationInterceptorObjName, "Passivations").toString()).equals(activationCount);
   }
}
