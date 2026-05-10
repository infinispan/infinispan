package org.infinispan.configuration;

import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.lookup.EmbeddedTransactionManagerLookup;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 5.1
 */
@Test (groups = "functional", testName = "configuration.TransactionalCacheConfigTest")
public class TransactionalCacheConfigTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      return TestCacheManagerFactory.createCacheManager(getDefaultStandaloneCacheConfig(true));
   }

   public void test() {
      final ConfigurationBuilder c = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      assertFalse(c.build().transaction().transactionMode().isTransactional());
      c.transaction().transactionMode(TransactionMode.TRANSACTIONAL);
      assertTrue(c.build().transaction().transactionMode().isTransactional());
      c.transaction().transactionMode(TransactionMode.NON_TRANSACTIONAL);
      assertFalse(c.build().transaction().transactionMode().isTransactional());
   }

   public void testTransactionModeOverride() {
      ConfigurationBuilder c = new ConfigurationBuilder();
      c.transaction().transactionMode(TransactionMode.NON_TRANSACTIONAL);
      assertEquals(TransactionMode.TRANSACTIONAL, cacheManager.getCache().getCacheConfiguration().transaction().transactionMode());
      cacheManager.defineConfiguration("nonTx", c.build());
      assertEquals(TransactionMode.NON_TRANSACTIONAL, cacheManager.getCache("nonTx").getCacheConfiguration().transaction().transactionMode());
   }

   public void testDefaults() {
      Configuration c = new ConfigurationBuilder().build();
      assertFalse(c.transaction().transactionMode().isTransactional());

      c = TestCacheManagerFactory.getDefaultCacheConfiguration(false).build();
      assertFalse(c.transaction().transactionMode().isTransactional());

      c = TestCacheManagerFactory.getDefaultCacheConfiguration(true).build();
      assertTrue(c.transaction().transactionMode().isTransactional());

      c = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false).build();
      assertFalse(c.transaction().transactionMode().isTransactional());

      c = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true).build();
      assertTrue(c.transaction().transactionMode().isTransactional());
   }

   public void testTransactionalityInduced() {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      Configuration c = cb.build();
      assertFalse(c.transaction().transactionMode().isTransactional());

      c = cb.transaction().transactionManagerLookup(new EmbeddedTransactionManagerLookup()).build();
      assertTrue(c.transaction().transactionMode().isTransactional());

      cb = new ConfigurationBuilder();
      cb.invocationBatching().enable();
      assertTrue(cb.build().transaction().transactionMode().isTransactional());
   }

   public void testInvocationBatchingAndInducedTm() {
      final ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.invocationBatching().enable();
      assertTrue(cb.build().transaction().transactionMode().isTransactional());
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.createCacheManager(cb)){
         @Override
         public void call() {
            assertNotNull(cm.getCache().getAdvancedCache().getTransactionManager());
         }
      });
   }

   public void testOverride() {
      final ConfigurationBuilder c = new ConfigurationBuilder();
      c.transaction().transactionMode(TransactionMode.TRANSACTIONAL)
            .transactionManagerLookup(new EmbeddedTransactionManagerLookup());

      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.createCacheManager()){
         @Override
         public void call() {
            cm.defineConfiguration("transactional", c.build());
            Cache<?, ?> cache = cm.getCache("transactional");
            assertTrue(cache.getCacheConfiguration().transaction().transactionMode().isTransactional());
         }
      });
   }

   public void testBatchingAndTransactionalCache() {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.invocationBatching().enable();
      final Configuration c = cb.build();

      assertTrue(c.invocationBatching().enabled());
      assertTrue(c.transaction().transactionMode().isTransactional());

      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.createCacheManager(new ConfigurationBuilder())) {
         @Override
         public void call() {
            assertFalse(cm.getCache().getCacheConfiguration().transaction().transactionMode().isTransactional());

            cm.defineConfiguration("a", c);
            final Cache<Object, Object> a = cm.getCache("a");

            assertTrue(a.getCacheConfiguration().invocationBatching().enabled());
            assertTrue(a.getCacheConfiguration().transaction().transactionMode().isTransactional());
         }
      });
   }
}
