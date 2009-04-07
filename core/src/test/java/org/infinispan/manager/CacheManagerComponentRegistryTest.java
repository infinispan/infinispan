package org.infinispan.manager;

import org.infinispan.Cache;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.config.Configuration;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.eviction.EvictionManager;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.interceptors.BatchingInterceptor;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.remoting.RpcManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.DummyTransactionManager;
import org.infinispan.transaction.DummyTransactionManagerLookup;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import javax.transaction.TransactionManager;

/**
 * @author Manik Surtani
 * @since 4.0
 */
@Test(groups = "functional", testName = "manager.CacheManagerComponentRegistryTest")
public class CacheManagerComponentRegistryTest {
   DefaultCacheManager cm;

   @AfterMethod(alwaysRun = true)
   public void tearDown() {
      TestingUtil.killCacheManagers(cm);
   }

   public void testForceSharedComponents() throws NamedCacheNotFoundException {
      Configuration defaultCfg = new Configuration();
      defaultCfg.setCacheMode(Configuration.CacheMode.REPL_SYNC);
      defaultCfg.setFetchInMemoryState(false);
      defaultCfg.setFetchInMemoryState(false);

      // cache manager with default configuration
      cm = TestCacheManagerFactory.createCacheManager(GlobalConfiguration.getClusteredDefault(), defaultCfg);

      // default cache with no overrides
      Cache c = cm.getCache();

      Configuration overrides = new Configuration();
      overrides.setTransactionManagerLookupClass(DummyTransactionManagerLookup.class.getName());
      cm.defineCache("transactional", overrides);
      Cache transactional = cm.getCache("transactional");

      // assert components.
      assert TestingUtil.extractComponent(c, TransactionManager.class) == null;
      assert TestingUtil.extractComponent(transactional, TransactionManager.class) instanceof DummyTransactionManager;

      // assert force-shared components
      assert TestingUtil.extractComponent(c, RpcManager.class) != null;
      assert TestingUtil.extractComponent(transactional, RpcManager.class) != null;
      assert TestingUtil.extractComponent(c, RpcManager.class) == TestingUtil.extractComponent(transactional, RpcManager.class);
   }

   public void testForceUnsharedComponents() throws NamedCacheNotFoundException {
      Configuration defaultCfg = new Configuration();
      defaultCfg.setFetchInMemoryState(false);
      defaultCfg.setCacheMode(Configuration.CacheMode.REPL_SYNC);
      defaultCfg.setEvictionStrategy(EvictionStrategy.NONE);
      // cache manager with default configuration
      cm = TestCacheManagerFactory.createCacheManager(GlobalConfiguration.getClusteredDefault(), defaultCfg);

      // default cache with no overrides
      Cache c = cm.getCache();

      Configuration overrides = new Configuration();
      overrides.setTransactionManagerLookupClass(DummyTransactionManagerLookup.class.getName());
      cm.defineCache("transactional", overrides);
      Cache transactional = cm.getCache("transactional");

      // assert components.
      assert TestingUtil.extractComponent(c, EvictionManager.class) != null;
      assert TestingUtil.extractComponent(transactional, EvictionManager.class) != null;
      assert TestingUtil.extractComponent(c, EvictionManager.class) != TestingUtil.extractComponent(transactional, EvictionManager.class);
   }

   public void testOverridingComponents() throws NamedCacheNotFoundException {
      Configuration defaultCfg = new Configuration();
      cm = TestCacheManagerFactory.createCacheManager(GlobalConfiguration.getClusteredDefault(), defaultCfg);

      // default cache with no overrides
      Cache c = cm.getCache();

      Configuration overrides = new Configuration();
      overrides.setInvocationBatchingEnabled(true);
      cm.defineCache("overridden", overrides);
      Cache overridden = cm.getCache("overridden");

      // assert components.
      assert !TestingUtil.extractComponent(c, InterceptorChain.class).containsInterceptorType(BatchingInterceptor.class);
      assert TestingUtil.extractComponent(overridden, InterceptorChain.class).containsInterceptorType(BatchingInterceptor.class);
   }
}
