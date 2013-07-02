package org.infinispan.tx;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.jmx.PerThreadMBeanServerLookup;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

import static org.infinispan.test.TestingUtil.existsObject;
import static org.infinispan.test.TestingUtil.getCacheObjectName;

/**
 * Ensures that there is no recovery manager registered in JMX by default.
 *
 * @author Manik Surtani
 * @since 5.2
 */
@Test(testName = "tx.NoRecoveryManagerByDefaultTest", groups = "functional")
public class NoRecoveryManagerByDefaultTest extends SingleCacheManagerTest {
   public void testNoRecoveryManager() throws Exception {
      assert cache.getCacheConfiguration().transaction().transactionMode().isTransactional();
      String jmxDomain = cacheManager.getCacheManagerConfiguration().globalJmxStatistics().domain();
      assert !existsObject(getCacheObjectName(jmxDomain, cache.getName(), "RecoveryManager"));
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.transaction().transactionMode(TransactionMode.TRANSACTIONAL);
      return TestCacheManagerFactory.createCacheManager(cb);
   }
}
