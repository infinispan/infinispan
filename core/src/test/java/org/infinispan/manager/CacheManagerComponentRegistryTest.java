package org.infinispan.manager;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.eviction.EvictionManager;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.interceptors.BatchingInterceptor;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.test.AbstractCacheTest;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.infinispan.transaction.tm.DummyTransactionManager;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import javax.transaction.TransactionManager;

/**
 * @author Manik Surtani
 * @since 4.0
 */
@Test(groups = "functional", testName = "manager.CacheManagerComponentRegistryTest")
public class CacheManagerComponentRegistryTest extends AbstractCacheTest {
   EmbeddedCacheManager cm;

   @AfterMethod
   public void tearDown() {
      TestingUtil.killCacheManagers(cm);
      cm = null;
   }

   public void testForceSharedComponents() {
      ConfigurationBuilder defaultCfg = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC);
      defaultCfg
         .clustering()
            .stateTransfer()
               .fetchInMemoryState(false)
         .transaction()
            .transactionMode(TransactionMode.NON_TRANSACTIONAL);

      // cache manager with default configuration
      cm = TestCacheManagerFactory.createClusteredCacheManager(defaultCfg);

      // default cache with no overrides
      Cache c = cm.getCache();

      ConfigurationBuilder overrides = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      overrides.transaction().transactionManagerLookup(new DummyTransactionManagerLookup());
      cm.defineConfiguration("transactional", overrides.build());
      Cache transactional = cm.getCache("transactional");

      // assert components.
      assert TestingUtil.extractComponent(c, TransactionManager.class) == null;
      assert TestingUtil.extractComponent(transactional, TransactionManager.class) instanceof DummyTransactionManager;

      // assert force-shared components
      assert TestingUtil.extractComponent(c, Transport.class) != null;
      assert TestingUtil.extractComponent(transactional, Transport.class) != null;
      assert TestingUtil.extractComponent(c, Transport.class) == TestingUtil.extractComponent(transactional, Transport.class);
   }

   public void testForceUnsharedComponents() {
      ConfigurationBuilder defaultCfg = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC);
      defaultCfg
         .clustering()
            .stateTransfer()
               .fetchInMemoryState(false)
            .eviction()
               .strategy(EvictionStrategy.NONE);
      // cache manager with default configuration
      cm = TestCacheManagerFactory.createClusteredCacheManager(defaultCfg);

      // default cache with no overrides
      Cache c = cm.getCache();

      ConfigurationBuilder overrides = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      overrides.transaction().transactionManagerLookup(new DummyTransactionManagerLookup());
      cm.defineConfiguration("transactional", overrides.build());
      Cache transactional = cm.getCache("transactional");

      // assert components.
      assert TestingUtil.extractComponent(c, EvictionManager.class) != null;
      assert TestingUtil.extractComponent(transactional, EvictionManager.class) != null;
      assert TestingUtil.extractComponent(c, EvictionManager.class) != TestingUtil.extractComponent(transactional, EvictionManager.class);
   }

   public void testOverridingComponents() {
      cm = TestCacheManagerFactory.createClusteredCacheManager();

      // default cache with no overrides
      Cache c = cm.getCache();

      ConfigurationBuilder overrides = new ConfigurationBuilder();
      overrides.invocationBatching().enable();
      cm.defineConfiguration("overridden", overrides.build());
      Cache overridden = cm.getCache("overridden");

      // assert components.
      assert !TestingUtil.extractComponent(c, InterceptorChain.class).containsInterceptorType(BatchingInterceptor.class);
      assert TestingUtil.extractComponent(overridden, InterceptorChain.class).containsInterceptorType(BatchingInterceptor.class);
   }
}
