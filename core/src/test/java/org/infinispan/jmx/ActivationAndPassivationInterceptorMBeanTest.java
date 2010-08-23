package org.infinispan.jmx;

import org.infinispan.Cache;
import org.infinispan.config.CacheLoaderManagerConfig;
import org.infinispan.config.Configuration;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.container.entries.InternalEntryFactory;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.loaders.CacheStore;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStore;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import javax.management.Attribute;
import javax.management.MBeanServer;
import javax.management.ObjectName;
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
   private static final String JMX_DOMAIN = ActivationAndPassivationInterceptorMBeanTest.class.getName();

   protected EmbeddedCacheManager createCacheManager() throws Exception {
      GlobalConfiguration globalConfiguration = GlobalConfiguration.getNonClusteredDefault();
      globalConfiguration.setMBeanServerLookup(PerThreadMBeanServerLookup.class.getName());
      globalConfiguration.setJmxDomain(JMX_DOMAIN);
      globalConfiguration.setExposeGlobalJmxStatistics(true);
      cacheManager = TestCacheManagerFactory.createCacheManagerEnforceJmxDomain(globalConfiguration);
      DummyInMemoryCacheStore.Cfg cfg = new DummyInMemoryCacheStore.Cfg();
      CacheLoaderManagerConfig clManagerConfig = new CacheLoaderManagerConfig();
      clManagerConfig.setPassivation(true);
      clManagerConfig.addCacheLoaderConfig(cfg);
      Configuration configuration = getDefaultClusteredConfig(Configuration.CacheMode.LOCAL);
      configuration.setExposeJmxStatistics(true);
      configuration.setCacheLoaderManagerConfig(clManagerConfig);

      cacheManager.defineConfiguration("test", configuration);
      cache = cacheManager.getCache("test");
      activationInterceptorObjName = new ObjectName(JMX_DOMAIN + ":cache-name=test(local),jmx-resource=Activation");
      passivationInterceptorObjName = new ObjectName(JMX_DOMAIN + ":cache-name=test(local),jmx-resource=Passivation");

      threadMBeanServer = PerThreadMBeanServerLookup.getThreadMBeanServer();
      cacheStore = TestingUtil.extractComponent(cache, CacheLoaderManager.class).getCacheStore();

      return cacheManager;
   }

   @AfterMethod
   public void resetStats() throws Exception {
      threadMBeanServer.invoke(activationInterceptorObjName, "resetStatistics", new Object[0], new String[0]);
   }

   public void testDisableStatistics() throws Exception {
      threadMBeanServer.setAttribute(activationInterceptorObjName, new Attribute("StatisticsEnabled", Boolean.FALSE));
      assert threadMBeanServer.getAttribute(activationInterceptorObjName, "Activations").toString().equals("N/A");
      threadMBeanServer.setAttribute(activationInterceptorObjName, new Attribute("StatisticsEnabled", Boolean.TRUE));
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
