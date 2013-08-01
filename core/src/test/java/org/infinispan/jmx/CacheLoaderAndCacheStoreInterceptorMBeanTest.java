package org.infinispan.jmx;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.test.fwk.TestInternalCacheEntryFactory;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStoreConfigurationBuilder;
import org.infinispan.loaders.manager.CacheLoaderManager;
import org.infinispan.loaders.spi.CacheStore;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import static org.infinispan.test.TestingUtil.*;

/**
 * Tests the jmx functionality from CacheLoaderInterceptor and CacheStoreInterceptor.
 *
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 */
@Test(groups = "functional", testName = "jmx.CacheLoaderAndCacheStoreInterceptorMBeanTest")
public class CacheLoaderAndCacheStoreInterceptorMBeanTest extends SingleCacheManagerTest {
   private ObjectName loaderInterceptorObjName;
   private ObjectName storeInterceptorObjName;
   private MBeanServer threadMBeanServer;
   private CacheStore cacheStore;
   private static final String JMX_DOMAIN = CacheLoaderAndCacheStoreInterceptorMBeanTest.class.getName();

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      cacheManager = TestCacheManagerFactory.createCacheManagerEnforceJmxDomain(JMX_DOMAIN);
      ConfigurationBuilder configuration = getDefaultStandaloneCacheConfig(false);
      configuration
         .jmxStatistics().enable()
         .loaders()
            .passivation(false)
            .addStore(DummyInMemoryCacheStoreConfigurationBuilder.class);

      cacheManager.defineConfiguration("test", configuration.build());
      cache = cacheManager.getCache("test");
      loaderInterceptorObjName = getCacheObjectName(JMX_DOMAIN, "test(local)", "CacheLoader");
      storeInterceptorObjName = getCacheObjectName(JMX_DOMAIN, "test(local)", "CacheStore");

      threadMBeanServer = PerThreadMBeanServerLookup.getThreadMBeanServer();
      cacheStore = TestingUtil.extractComponent(cache, CacheLoaderManager.class).getCacheStore();
      return cacheManager;
   }

   @AfterMethod
   public void resetStats() throws Exception {
      threadMBeanServer.invoke(loaderInterceptorObjName, "resetStatistics", new Object[0], new String[0]);
      threadMBeanServer.invoke(storeInterceptorObjName, "resetStatistics", new Object[0], new String[0]);
   }

   public void testJmxOperationMetadata() throws Exception {
      checkMBeanOperationParameterNaming(loaderInterceptorObjName);
      checkMBeanOperationParameterNaming(storeInterceptorObjName);
   }

   public void testPutKeyValue() throws Exception {
      assertStoreAccess(0, 0, 0);
      cache.put("key", "value");
      assertStoreAccess(0, 0, 1);
      cache.put("key", "value2");
      assertStoreAccess(0, 0, 2);

      cacheStore.store(TestInternalCacheEntryFactory.create("a", "b"));
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

      cacheStore.store(TestInternalCacheEntryFactory.create("a", "b"));
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

      cacheStore.store(TestInternalCacheEntryFactory.create("a", "b"));
      assert cache.remove("a").equals("b");
      assertStoreAccess(1, 1, 1);
   }

   public void testReplaceCommand() throws Exception {
      assertStoreAccess(0, 0, 0);
      cache.put("key", "value");
      assertStoreAccess(0, 0, 1);

      assert cache.replace("key", "value2").equals("value");
      assertStoreAccess(0, 0, 2);

      cacheStore.store(TestInternalCacheEntryFactory.create("a", "b"));
      assert cache.replace("a", "c").equals("b");
      assertStoreAccess(1, 0, 3);

      assert cache.replace("no_such_key", "c") == null;
      assertStoreAccess(1, 1, 3);
   }

   public void testFlagMissNotCounted() throws Exception {
      assertStoreAccess(0, 0, 0);
      cache.put("key", "value");
      assertStoreAccess(0, 0, 1);
      cache.getAdvancedCache().withFlags(Flag.SKIP_CACHE_LOAD).get("no_such_key");
      assertStoreAccess(0, 0, 1);
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
