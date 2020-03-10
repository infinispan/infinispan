package org.infinispan.jmx;

import static org.infinispan.test.TestingUtil.checkMBeanOperationParameterNaming;
import static org.infinispan.test.TestingUtil.getCacheObjectName;

import javax.management.ObjectName;

import org.infinispan.commons.jmx.MBeanServerLookup;
import org.infinispan.commons.jmx.TestMBeanServerLookup;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.lookup.EmbeddedTransactionManagerLookup;
import org.infinispan.transaction.tm.EmbeddedTransactionManager;
import org.testng.annotations.Test;

/**
 * Test the JMX functionality in {@link org.infinispan.util.concurrent.locks.LockManager}.
 *
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 */
@Test(groups = "functional", testName = "jmx.MvccLockManagerMBeanTest")
public class MvccLockManagerMBeanTest extends SingleCacheManagerTest {
   private static final int CONCURRENCY_LEVEL = 129;

   private ObjectName lockManagerObjName;
   private final MBeanServerLookup mBeanServerLookup = TestMBeanServerLookup.create();
   private static final String JMX_DOMAIN = MvccLockManagerMBeanTest.class.getSimpleName();

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder configuration = getDefaultStandaloneCacheConfig(true);
      configuration
            .locking()
               .concurrencyLevel(CONCURRENCY_LEVEL)
               .useLockStriping(true)
            .transaction()
               .transactionManagerLookup(new EmbeddedTransactionManagerLookup());

      GlobalConfigurationBuilder globalConfiguration = new GlobalConfigurationBuilder();
      globalConfiguration
            .jmx().enabled(true)
            .domain(JMX_DOMAIN)
            .mBeanServerLookup(mBeanServerLookup);

      cacheManager = TestCacheManagerFactory.createCacheManager(globalConfiguration, configuration);

      cacheManager.defineConfiguration("test", configuration.build());
      cache = cacheManager.getCache("test");
      lockManagerObjName = getCacheObjectName(JMX_DOMAIN, "test(local)", "LockManager");

      return cacheManager;
   }

   public void testJmxOperationMetadata() throws Exception {
      checkMBeanOperationParameterNaming(mBeanServerLookup.getMBeanServer(), lockManagerObjName);
   }

   public void testConcurrencyLevel() throws Exception {
      assertAttributeValue("ConcurrencyLevel", CONCURRENCY_LEVEL);
   }

   public void testNumberOfLocksHeld() throws Exception {
      EmbeddedTransactionManager tm = (EmbeddedTransactionManager) tm();
      tm.begin();
      cache.put("key", "value");
      tm.getTransaction().runPrepare();
      assertAttributeValue("NumberOfLocksHeld", 1);
      tm.getTransaction().runCommit(false);
      assertAttributeValue("NumberOfLocksHeld", 0);
   }

   public void testNumberOfLocksAvailable() throws Exception {
      EmbeddedTransactionManager tm = (EmbeddedTransactionManager) tm();
      int initialAvailable = getAttrValue("NumberOfLocksAvailable");
      tm.begin();
      cache.put("key", "value");
      tm.getTransaction().runPrepare();

      assertAttributeValue("NumberOfLocksHeld", 1);
      assertAttributeValue("NumberOfLocksAvailable", initialAvailable - 1);
      tm.getTransaction().runCommit(true);
      assertAttributeValue("NumberOfLocksAvailable", initialAvailable);
      assertAttributeValue("NumberOfLocksHeld", 0);
   }

   private void assertAttributeValue(String attrName, int expectedVal) throws Exception {
      int cl = getAttrValue(attrName);
      assert cl == expectedVal : "expected " + expectedVal + ", but received " + cl;
   }

   private int getAttrValue(String attrName) throws Exception {
      return Integer.parseInt(mBeanServerLookup.getMBeanServer().getAttribute(lockManagerObjName, attrName).toString());
   }
}
