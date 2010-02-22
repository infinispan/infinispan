package org.infinispan.manager;

import org.infinispan.Cache;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.config.Configuration;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.lifecycle.ComponentStatus;
import org.testng.annotations.Test;

/**
 * @author Manik Surtani
 * @since 4.0
 */
@Test(groups = "functional", testName = "manager.CacheManagerTest")
public class CacheManagerTest extends AbstractInfinispanTest {
   public void testDefaultCache() {
      CacheManager cm = TestCacheManagerFactory.createLocalCacheManager();

      try {
         assert cm.getCache().getStatus() == ComponentStatus.RUNNING;
         assert cm.getCache().getName().equals(DefaultCacheManager.DEFAULT_CACHE_NAME);

         try {
            cm.defineConfiguration(DefaultCacheManager.DEFAULT_CACHE_NAME, new Configuration());
            assert false : "Should fail";
         }
         catch (IllegalArgumentException e) {
            assert true; // ok
         }
      } finally {
         TestingUtil.killCacheManagers(cm);
      }
   }

   public void testClashingNames() {
      CacheManager cm = TestCacheManagerFactory.createLocalCacheManager();
      try {
         Configuration c = new Configuration();
         Configuration firstDef = cm.defineConfiguration("aCache", c);
         Configuration secondDef = cm.defineConfiguration("aCache", c);
         assert firstDef.equals(secondDef);
      } finally {
         TestingUtil.killCacheManagers(cm);
      }
   }

   public void testStartAndStop() {
      CacheManager cm = TestCacheManagerFactory.createLocalCacheManager();
      try {
         Cache c1 = cm.getCache("cache1");
         Cache c2 = cm.getCache("cache2");
         Cache c3 = cm.getCache("cache3");

         assert c1.getStatus() == ComponentStatus.RUNNING;
         assert c2.getStatus() == ComponentStatus.RUNNING;
         assert c3.getStatus() == ComponentStatus.RUNNING;

         cm.stop();

         assert c1.getStatus() == ComponentStatus.TERMINATED;
         assert c2.getStatus() == ComponentStatus.TERMINATED;
         assert c3.getStatus() == ComponentStatus.TERMINATED;
      } finally {
         TestingUtil.killCacheManagers(cm);
      }
   }

   public void testDefiningConfigurationValidation() {
      CacheManager cm = TestCacheManagerFactory.createLocalCacheManager();
      try {
         cm.defineConfiguration("cache1", null);
         assert false : "Should fail";
      } catch(NullPointerException npe) {
         assert npe.getMessage() != null;
      }
      
      try {
         cm.defineConfiguration(null, null);
         assert false : "Should fail";
      } catch(NullPointerException npe) {
         assert npe.getMessage() != null;
      }
      
      try {
         cm.defineConfiguration(null, new Configuration());
         assert false : "Should fail";
      } catch(NullPointerException npe) {
         assert npe.getMessage() != null;
      }
      
      Configuration c = cm.defineConfiguration("cache1", null, new Configuration());
      assert c.equals(cm.getDefaultConfiguration());
      
      c = cm.defineConfiguration("cache1", "does-not-exist-cache", new Configuration());
      assert c.equals(cm.getDefaultConfiguration());
   }

   public void testDefiningConfigurationWithTemplateName() {
      CacheManager cm = TestCacheManagerFactory.createLocalCacheManager();

      Configuration c = new Configuration();
      c.setIsolationLevel(IsolationLevel.NONE);
      Configuration oneCacheConfiguration = cm.defineConfiguration("oneCache", c);
      assert oneCacheConfiguration.equals(c);
      assert oneCacheConfiguration.getIsolationLevel().equals(IsolationLevel.NONE);
      
      c = new Configuration();
      Configuration secondCacheConfiguration = cm.defineConfiguration("secondCache", "oneCache", c);
      assert oneCacheConfiguration.equals(secondCacheConfiguration);
      assert secondCacheConfiguration.getIsolationLevel().equals(IsolationLevel.NONE);
      
      c = new Configuration();
      c.setIsolationLevel(IsolationLevel.SERIALIZABLE);
      Configuration anotherSecondCacheConfiguration = cm.defineConfiguration("secondCache", "oneCache", c);
      assert !secondCacheConfiguration.equals(anotherSecondCacheConfiguration);
      assert anotherSecondCacheConfiguration.getIsolationLevel().equals(IsolationLevel.SERIALIZABLE);
      assert secondCacheConfiguration.getIsolationLevel().equals(IsolationLevel.NONE);
      
      c = new Configuration();
      c.setExpirationMaxIdle(Long.MAX_VALUE);
      Configuration yetAnotherSecondCacheConfiguration = cm.defineConfiguration("secondCache", "oneCache", c);
      assert yetAnotherSecondCacheConfiguration.getIsolationLevel().equals(IsolationLevel.NONE);
      assert yetAnotherSecondCacheConfiguration.getExpirationMaxIdle() == Long.MAX_VALUE;
      assert secondCacheConfiguration.getIsolationLevel().equals(IsolationLevel.NONE);
      assert anotherSecondCacheConfiguration.getIsolationLevel().equals(IsolationLevel.SERIALIZABLE);
   }

   public void testDefiningConfigurationOverridingBooleans() {
      CacheManager cm = TestCacheManagerFactory.createLocalCacheManager();
      Configuration c = new Configuration();
      c.setUseLazyDeserialization(true);
      Configuration lazy = cm.defineConfiguration("lazyDeserialization", c);
      assert lazy.isUseLazyDeserialization();

      c = new Configuration();
      c.setEvictionStrategy(EvictionStrategy.LRU);
      Configuration lazyLru = cm.defineConfiguration("lazyDeserializationWithLRU", "lazyDeserialization", c);
      assert lazyLru.isUseLazyDeserialization();
      assert lazyLru.getEvictionStrategy() == EvictionStrategy.LRU;
   }
}
