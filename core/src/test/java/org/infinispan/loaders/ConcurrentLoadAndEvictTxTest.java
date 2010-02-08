package org.infinispan.loaders;

import org.infinispan.config.CacheLoaderManagerConfig;
import org.infinispan.config.Configuration;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStore;
import org.infinispan.manager.CacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;

import static org.infinispan.test.TestingUtil.getTransactionManager;

@Test(groups = "functional", testName = "loaders.ConcurrentLoadAndEvictTxTest")
public class ConcurrentLoadAndEvictTxTest extends SingleCacheManagerTest {

   TransactionManager tm;

   protected CacheManager createCacheManager() throws Exception {
      Configuration config = new Configuration();
      config.setEvictionStrategy(EvictionStrategy.FIFO);
      config.setEvictionWakeUpInterval(10);
      config.setEvictionMaxEntries(1);
      CacheLoaderManagerConfig clmc = new CacheLoaderManagerConfig();
      config.setCacheLoaderManagerConfig(clmc);
      clmc.addCacheLoaderConfig(new DummyInMemoryCacheStore.Cfg());
      CacheManager cm = TestCacheManagerFactory.createCacheManager(config, true);
      cache = cm.getCache();
      tm = getTransactionManager(cache);
      return cm;
   }

   public void testEvictAndTx() throws SystemException, NotSupportedException, RollbackException, HeuristicRollbackException, HeuristicMixedException {
      for (int i=0; i<10000; i++) {
         tm.begin();
         for (int j=0; j<10; j++) cache.put(String.format("key-%s-%s", i, j), "value");
         tm.commit();
         for (int j=0; j<10; j++) assert "value".equals(cache.get(String.format("key-%s-%s", i, j))) : "Data loss on key " + String.format("key-%s-%s", i, j);
      }
   }

}