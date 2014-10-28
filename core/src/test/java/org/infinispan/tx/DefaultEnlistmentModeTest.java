package org.infinispan.tx;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.test.AbstractCacheTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@Test (groups = "functional", testName = "tx.DefaultEnlistmentModeTest")
public class DefaultEnlistmentModeTest extends AbstractCacheTest {

   private DefaultCacheManager dcm;

   @AfterMethod
   protected void destroyCacheManager() {
      TestingUtil.killCacheManagers(dcm);
   }

   public void testDefaultEnlistment() {
      ConfigurationBuilder builder = getLocalBuilder();
      builder.transaction().transactionMode(TransactionMode.TRANSACTIONAL);
      dcm = new DefaultCacheManager(getGlobalConfig(), builder.build());
      Cache<Object,Object> cache = dcm.getCache();
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
      dcm = new DefaultCacheManager(getGlobalConfig(), builder.build());
      Cache<Object,Object> cache = dcm.getCache();
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
      dcm = new DefaultCacheManager(getGlobalConfig(), builder.build());
      Cache<Object,Object> cache = dcm.getCache();
      assertFalse(cache.getCacheConfiguration().transaction().useSynchronization());
      assertFalse(cache.getCacheConfiguration().transaction().recovery().enabled());
   }

   private ConfigurationBuilder getLocalBuilder() {
      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      builder.clustering().cacheMode(CacheMode.LOCAL);
      return builder;
   }

   private GlobalConfiguration getGlobalConfig() {
      return new GlobalConfigurationBuilder().globalJmxStatistics().allowDuplicateDomains(true).build();
   }
}
