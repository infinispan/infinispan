package org.infinispan.manager;

import javax.transaction.TransactionManager;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.eviction.EvictionManager;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.interceptors.impl.BatchingInterceptor;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.test.AbstractCacheTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.lookup.EmbeddedTransactionManagerLookup;
import org.infinispan.transaction.tm.EmbeddedTransactionManager;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * @author Manik Surtani
 * @since 4.0
 */
@Test(groups = "functional", testName = "manager.CacheManagerComponentRegistryTest")
public class CacheManagerComponentRegistryTest extends AbstractCacheTest {
   private EmbeddedCacheManager cm;

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
      overrides.transaction().transactionManagerLookup(new EmbeddedTransactionManagerLookup());
      cm.defineConfiguration("transactional", overrides.build());
      Cache transactional = cm.getCache("transactional");

      // assert components.
      assert TestingUtil.extractComponent(c, TransactionManager.class) == null;
      assert TestingUtil.extractComponent(transactional, TransactionManager.class) instanceof EmbeddedTransactionManager;

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
               .fetchInMemoryState(false);
      // cache manager with default configuration
      cm = TestCacheManagerFactory.createClusteredCacheManager(defaultCfg);

      // default cache with no overrides
      Cache c = cm.getCache();

      ConfigurationBuilder overrides = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      overrides.transaction().transactionManagerLookup(new EmbeddedTransactionManagerLookup());
      cm.defineConfiguration("transactional", overrides.build());
      Cache transactional = cm.getCache("transactional");

      // assert components.
      assert TestingUtil.extractComponent(c, EvictionManager.class) != null;
      assert TestingUtil.extractComponent(transactional, EvictionManager.class) != null;
      assert TestingUtil.extractComponent(c, EvictionManager.class) != TestingUtil.extractComponent(transactional, EvictionManager.class);
   }

   public void testOverridingComponents() {
      cm = TestCacheManagerFactory.createClusteredCacheManager(new ConfigurationBuilder());

      // default cache with no overrides
      Cache c = cm.getCache();

      ConfigurationBuilder overrides = new ConfigurationBuilder();
      overrides.invocationBatching().enable();
      cm.defineConfiguration("overridden", overrides.build());
      Cache overridden = cm.getCache("overridden");

      // assert components.
      AsyncInterceptorChain initialChain = c.getAdvancedCache().getAsyncInterceptorChain();
      assert !initialChain.containsInterceptorType(BatchingInterceptor.class);
      AsyncInterceptorChain overriddenChain = overridden.getAdvancedCache().getAsyncInterceptorChain();
      assert overriddenChain.containsInterceptorType(BatchingInterceptor.class);
   }
}
