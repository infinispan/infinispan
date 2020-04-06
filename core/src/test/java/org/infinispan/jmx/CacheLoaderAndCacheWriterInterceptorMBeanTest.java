package org.infinispan.jmx;

import static org.infinispan.test.TestingUtil.checkMBeanOperationParameterNaming;
import static org.infinispan.test.TestingUtil.getCacheObjectName;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.infinispan.commons.jmx.MBeanServerLookup;
import org.infinispan.commons.jmx.TestMBeanServerLookup;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.persistence.impl.MarshalledEntryUtil;
import org.infinispan.persistence.dummy.DummyInMemoryStore;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * Tests the jmx functionality from CacheLoaderInterceptor and CacheWriterInterceptor.
 *
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 */
@Test(groups = "functional", testName = "jmx.CacheLoaderAndCacheWriterInterceptorMBeanTest")
public class CacheLoaderAndCacheWriterInterceptorMBeanTest extends SingleCacheManagerTest {
   private static final String JMX_DOMAIN = CacheLoaderAndCacheWriterInterceptorMBeanTest.class.getName();

   private ObjectName loaderInterceptorObjName;
   private ObjectName storeInterceptorObjName;
   private DummyInMemoryStore store;
   private final MBeanServerLookup mBeanServerLookup = TestMBeanServerLookup.create();

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder configuration = getDefaultStandaloneCacheConfig(false);
      configuration
            .statistics().enable()
            .persistence()
            .passivation(false)
            .addStore(DummyInMemoryStoreConfigurationBuilder.class);

      GlobalConfigurationBuilder globalConfiguration = new GlobalConfigurationBuilder();
      globalConfiguration
            .cacheContainer().statistics(true)
            .jmx().enabled(true).domain(JMX_DOMAIN).mBeanServerLookup(mBeanServerLookup);
      cacheManager = TestCacheManagerFactory.createCacheManager(globalConfiguration, configuration);

      cacheManager.defineConfiguration("test", configuration.build());
      cache = cacheManager.getCache("test");
      loaderInterceptorObjName = getCacheObjectName(JMX_DOMAIN, "test(local)", "CacheLoader");
      storeInterceptorObjName = getCacheObjectName(JMX_DOMAIN, "test(local)", "CacheStore");

      store = TestingUtil.getFirstStore(cache);
      return cacheManager;
   }

   @AfterMethod
   public void resetStats() throws Exception {
      MBeanServer mBeanServer = mBeanServerLookup.getMBeanServer();
      mBeanServer.invoke(loaderInterceptorObjName, "resetStatistics", new Object[0], new String[0]);
      mBeanServer.invoke(storeInterceptorObjName, "resetStatistics", new Object[0], new String[0]);
   }

   public void testJmxOperationMetadata() throws Exception {
      MBeanServer mBeanServer = mBeanServerLookup.getMBeanServer();
      checkMBeanOperationParameterNaming(mBeanServer, loaderInterceptorObjName);
      checkMBeanOperationParameterNaming(mBeanServer, storeInterceptorObjName);
   }

   public void testPutKeyValue() throws Exception {
      assertStoreAccess(0, 0, 0);
      cache.put("key", "value");
      assertStoreAccess(0, 1, 1);
      cache.put("key", "value2");
      assertStoreAccess(0, 1, 2);

      store.write(MarshalledEntryUtil.create("a", "b", cache));
      cache.put("a", "c");
      assertStoreAccess(1, 1, 3);
      assert store.loadEntry("a").getValue().equals("c");
   }

   public void testGetValue() throws Exception {
      assertStoreAccess(0, 0, 0);
      cache.put("key", "value");
      assertStoreAccess(0, 1, 1);

      assert cache.get("key").equals("value");
      assertStoreAccess(0, 1, 1);

      store.write(MarshalledEntryUtil.create("a", "b", cache));
      assert cache.get("a").equals("b");
      assertStoreAccess(1, 1, 1);

      assert cache.get("no_such_key") == null;
      assertStoreAccess(1, 2, 1);
   }

   public void testRemoveValue() throws Exception {
      assertStoreAccess(0, 0, 0);
      cache.put("key", "value");
      assertStoreAccess(0, 1, 1);

      assert cache.get("key").equals("value");
      assertStoreAccess(0, 1, 1);

      assert cache.remove("key").equals("value");
      assertStoreAccess(0, 1, 1);

      cache.remove("no_such_key");
      assertStoreAccess(0, 2, 1);

      store.write(MarshalledEntryUtil.create("a", "b", cache));
      assert cache.remove("a").equals("b");
      assertStoreAccess(1, 2, 1);
   }

   public void testReplaceCommand() throws Exception {
      assertStoreAccess(0, 0, 0);
      cache.put("key", "value");
      assertStoreAccess(0, 1, 1);

      assert cache.replace("key", "value2").equals("value");
      assertStoreAccess(0, 1, 2);

      store.write(MarshalledEntryUtil.create("a", "b", cache));
      assert cache.replace("a", "c").equals("b");
      assertStoreAccess(1, 1, 3);

      assert cache.replace("no_such_key", "c") == null;
      assertStoreAccess(1, 2, 3);
   }

   public void testFlagMissNotCounted() throws Exception {
      assertStoreAccess(0, 0, 0);
      cache.put("key", "value");
      assertStoreAccess(0, 1, 1);
      cache.getAdvancedCache().withFlags(Flag.SKIP_CACHE_LOAD).get("no_such_key");
      assertStoreAccess(0, 1, 1);
   }

   private void assertStoreAccess(int loadsCount, int missesCount, int storeCount) throws Exception {
      assertLoadCount(loadsCount, missesCount);
      assertStoreCount(storeCount);
   }

   private void assertLoadCount(int loadsCount, int missesCount) throws Exception {
      MBeanServer mBeanServer = mBeanServerLookup.getMBeanServer();
      String actualLoadCount = mBeanServer.getAttribute(loaderInterceptorObjName, "CacheLoaderLoads").toString();
      assert Integer.valueOf(actualLoadCount).equals(loadsCount) : "expected " + loadsCount + " loads count and received " + actualLoadCount;
      String actualMissesCount = mBeanServer.getAttribute(loaderInterceptorObjName, "CacheLoaderMisses").toString();
      assert Integer.valueOf(actualMissesCount).equals(missesCount) : "expected " + missesCount + " misses count, and received " + actualMissesCount;
   }

   private void assertStoreCount(int count) throws Exception {
      String actualStoreCount = mBeanServerLookup.getMBeanServer().getAttribute(storeInterceptorObjName, "WritesToTheStores").toString();
      assert Integer.valueOf(actualStoreCount).equals(count) : "expected " + count + " store counts, but received " + actualStoreCount;
   }
}
