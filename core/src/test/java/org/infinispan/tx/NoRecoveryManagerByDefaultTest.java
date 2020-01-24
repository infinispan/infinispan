package org.infinispan.tx;

import static org.infinispan.test.TestingUtil.getCacheObjectName;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import javax.management.ObjectName;

import org.infinispan.commons.jmx.MBeanServerLookup;
import org.infinispan.commons.jmx.TestMBeanServerLookup;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

/**
 * Ensures that there is no recovery manager registered in JMX by default.
 *
 * @author Manik Surtani
 * @since 5.2
 */
@Test(testName = "tx.NoRecoveryManagerByDefaultTest", groups = "functional")
public class NoRecoveryManagerByDefaultTest extends SingleCacheManagerTest {

   private final MBeanServerLookup mBeanServerLookup = TestMBeanServerLookup.create();

   public void testNoRecoveryManager() {
      assertTrue(cache.getCacheConfiguration().transaction().transactionMode().isTransactional());
      String jmxDomain = cacheManager.getCacheManagerConfiguration().jmx().domain();
      ObjectName recoveryManager = getCacheObjectName(jmxDomain, cache.getName() + "(local)", "RecoveryManager");
      assertFalse(mBeanServerLookup.getMBeanServer().isRegistered(recoveryManager));
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      GlobalConfigurationBuilder gcb = new GlobalConfigurationBuilder().nonClusteredDefault();
      gcb.jmx().enabled(true).mBeanServerLookup(mBeanServerLookup);
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.transaction().transactionMode(TransactionMode.TRANSACTIONAL);
      return TestCacheManagerFactory.createCacheManager(gcb, cb);
   }
}
