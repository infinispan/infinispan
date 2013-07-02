package org.infinispan.config;

import java.lang.reflect.Method;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "config.ConfigurationCloneTest")
public class ConfigurationCloneTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      return TestCacheManagerFactory.createLocalCacheManager(false);
   }

   public void testCloningBeforeStart(Method method) {
      Configuration defaultConfig = cacheManager.defineConfiguration("default", new Configuration());
      Configuration clone = defaultConfig.clone();
      assert clone.equals(defaultConfig);
      clone.setEvictionMaxEntries(123);
      String name = method.getName() + "-default";
      cacheManager.defineConfiguration(name, clone);
      cacheManager.getCache(name);
   }

   public void testCloningAfterStart(Method method) {
      Configuration defaultConfig = cacheManager.getCache("default").getConfiguration();
      Configuration clone = defaultConfig.clone();
      assert clone.equals(defaultConfig);
      clone.setEvictionMaxEntries(456);
      String name = method.getName() + "-default";
      cacheManager.defineConfiguration(name, clone);
      cacheManager.getCache(name);
   }

   public void testDoubleCloning(Method method) {
      String name = method.getName();
      Configuration defaultConfig = cacheManager.defineConfiguration(name + "-default", new Configuration());
      Configuration clone = defaultConfig.clone();
      assert clone.equals(defaultConfig);
      clone.setEvictionMaxEntries(789);
      cacheManager.defineConfiguration(name + "-new-default", clone);
      cacheManager.getCache(name + "-new-default");

      Configuration otherDefaultConfig = cacheManager.getCache(name + "-default").getConfiguration();
      Configuration otherClone = otherDefaultConfig.clone();
      assert otherClone.equals(otherDefaultConfig);
      otherClone.setEvictionMaxEntries(788);

      try {
         cacheManager.defineConfiguration(name + "-new-default", otherClone);
      } catch (CacheConfigurationException e) {
         String message = e.getMessage();
         assert message.contains("[maxEntries]") : "Exception should indicate that it's Eviction maxEntries that we're trying to override but it says: " + message;
      }
   }

   public void testGlobalConfigurationCloning(Method m) {
      GlobalConfiguration clone = cacheManager.getGlobalConfiguration().clone();
      String newJmxDomain = m.getName();
      clone.setJmxDomain(newJmxDomain);
      EmbeddedCacheManager cacheManager = TestCacheManagerFactory.createCacheManagerEnforceJmxDomain(clone);
      assert cacheManager.getGlobalConfiguration().getJmxDomain().equals(newJmxDomain);
   }
}
