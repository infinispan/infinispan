package org.infinispan.configuration;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.testng.annotations.Test;

import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.testng.Assert.assertEquals;

/**
 * @author Mircea Markus
 * @since 5.1
 */
@Test (groups = "functional", testName = "config.TransactionalCacheConfigTest")
public class TransactionalCacheConfigTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      return TestCacheManagerFactory.createCacheManager(getDefaultStandaloneCacheConfig(true));
   }

   public void test() {
      final ConfigurationBuilder c = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      assert !c.build().transaction().transactionMode().isTransactional();
      c.transaction().transactionMode(TransactionMode.TRANSACTIONAL);
      assert c.build().transaction().transactionMode().isTransactional();
      c.transaction().transactionMode(TransactionMode.NON_TRANSACTIONAL);
      assert !c.build().transaction().transactionMode().isTransactional();
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
      assert !c.transaction().transactionMode().isTransactional();

      c = TestCacheManagerFactory.getDefaultCacheConfiguration(false).build();
      assert !c.transaction().transactionMode().isTransactional();

      c = TestCacheManagerFactory.getDefaultCacheConfiguration(true).build();
      assert c.transaction().transactionMode().isTransactional();

      c = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false).build();
      assert !c.transaction().transactionMode().isTransactional();

      c = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true).build();
      assert c.transaction().transactionMode().isTransactional();
   }

   public void testTransactionalityInduced() {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      Configuration c = cb.build();
      assert !c.transaction().transactionMode().isTransactional();

      c = cb.transaction().transactionManagerLookup(new DummyTransactionManagerLookup()).build();
      assert c.transaction().transactionMode().isTransactional();

      cb = new ConfigurationBuilder();
      cb.invocationBatching().enable();
      assert cb.build().transaction().transactionMode().isTransactional();
   }

   public void testInvocationBatchingAndInducedTm() {
      final ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.invocationBatching().enable();
      assert cb.build().transaction().transactionMode().isTransactional();
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.createCacheManager(cb)){
         @Override
         public void call() {
            assert cm.getCache().getAdvancedCache().getTransactionManager() != null;
         }
      });
   }

   public void testOverride() {
      final ConfigurationBuilder c = new ConfigurationBuilder();
      c.transaction().transactionMode(TransactionMode.TRANSACTIONAL)
            .transactionManagerLookup(new DummyTransactionManagerLookup());

      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.createCacheManager()){
         @Override
         public void call() {
            cm.defineConfiguration("transactional", c.build());
            Cache<?, ?> cache = cm.getCache("transactional");
            assert cache.getCacheConfiguration().transaction().transactionMode().isTransactional();
         }
      });
   }

   public void testBatchingAndTransactionalCache() {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.invocationBatching().enable();
      final Configuration c = cb.build();

      assert c.invocationBatching().enabled();
      assert c.transaction().transactionMode().isTransactional();

      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.createCacheManager()) {
         @Override
         public void call() {
            assert !cm.getCache().getCacheConfiguration().transaction().transactionMode().isTransactional();

            cm.defineConfiguration("a", c);
            final Cache<Object, Object> a = cm.getCache("a");

            assert a.getCacheConfiguration().invocationBatching().enabled();
            assert a.getCacheConfiguration().transaction().transactionMode().isTransactional();
         }
      });
   }

   private void assertTmLookupSet(Configuration c, boolean b) {
      assert b == (c.transaction().transactionManagerLookup() != null);
   }
}
