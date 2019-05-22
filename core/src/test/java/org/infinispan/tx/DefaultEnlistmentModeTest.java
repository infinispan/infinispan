package org.infinispan.tx;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractCacheTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

@Test (groups = "functional", testName = "tx.DefaultEnlistmentModeTest")
public class DefaultEnlistmentModeTest extends AbstractCacheTest {

   private EmbeddedCacheManager ecm;

   @AfterMethod
   protected void destroyCacheManager() {
      TestingUtil.killCacheManagers(ecm);
   }

   public void testDefaultEnlistment() {
      ConfigurationBuilder builder = getLocalBuilder();
      builder.transaction().transactionMode(TransactionMode.TRANSACTIONAL);
      ecm = TestCacheManagerFactory.createCacheManager(builder);
      Cache<Object,Object> cache = ecm.getCache();
      assertFalse(cache.getCacheConfiguration().transaction().useSynchronization());
      assertFalse(cache.getCacheConfiguration().transaction().recovery().enabled());
      cache.put("k", "v");
      assertEquals("v", cache.get("k"));
   }

   public void testXAEnlistment() {
      ConfigurationBuilder builder = getLocalBuilder();
      builder.transaction()
            .transactionMode(TransactionMode.TRANSACTIONAL)
            .useSynchronization(false);
      ecm =  TestCacheManagerFactory.createCacheManager(builder);
      Cache<Object,Object> cache = ecm.getCache();
      assertFalse(cache.getCacheConfiguration().transaction().useSynchronization());
      assertFalse(cache.getCacheConfiguration().transaction().recovery().enabled());
      cache.put("k", "v");
      assertEquals("v", cache.get("k"));
   }

   public void testXAEnlistmentNoRecovery() {
      ConfigurationBuilder builder = getLocalBuilder();
      builder.transaction()
            .transactionMode(TransactionMode.TRANSACTIONAL)
            .useSynchronization(false)
            .recovery().disable();
      ecm = TestCacheManagerFactory.createCacheManager(builder);
      Cache<Object,Object> cache = ecm.getCache();
      assertFalse(cache.getCacheConfiguration().transaction().useSynchronization());
      assertFalse(cache.getCacheConfiguration().transaction().recovery().enabled());
   }

   private ConfigurationBuilder getLocalBuilder() {
      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      builder.clustering().cacheMode(CacheMode.LOCAL);
      return builder;
   }

}
