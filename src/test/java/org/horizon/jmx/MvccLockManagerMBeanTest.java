package org.horizon.jmx;

import org.horizon.config.Configuration;
import org.horizon.config.GlobalConfiguration;
import org.horizon.manager.CacheManager;
import org.horizon.test.SingleCacheManagerTest;
import org.horizon.test.TestingUtil;
import org.horizon.test.fwk.TestCacheManagerFactory;
import org.horizon.transaction.DummyTransactionManagerLookup;
import org.testng.annotations.Test;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.transaction.TransactionManager;

/**
 * Test the JMX functionality in {@link org.horizon.lock.LockManagerImpl}.
 *
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "jmx.MvccLockManagerMBeanTest")
public class MvccLockManagerMBeanTest extends SingleCacheManagerTest {
   public static final int CONCURRENCY_LEVEL = 129;

   private ObjectName lockManagerObjName;
   private MBeanServer threadMBeanServer;

   protected CacheManager createCacheManager() throws Exception {
      GlobalConfiguration globalConfiguration = GlobalConfiguration.getNonClusteredDefault();
      globalConfiguration.setExposeGlobalJmxStatistics(true);
      globalConfiguration.setMBeanServerLookup(PerThreadMBeanServerLookup.class.getName());
      globalConfiguration.setJmxDomain("MvccLockManagerMBeanTest");
      cacheManager = TestCacheManagerFactory.createCacheManager(globalConfiguration);

      Configuration configuration = getDefaultClusteredConfig(Configuration.CacheMode.LOCAL);
      configuration.setExposeJmxStatistics(true);
      configuration.setTransactionManagerLookupClass(DummyTransactionManagerLookup.class.getName());
      configuration.setConcurrencyLevel(CONCURRENCY_LEVEL);

      cacheManager.defineCache("test", configuration);
      cache = cacheManager.getCache("test");
      lockManagerObjName = new ObjectName("MvccLockManagerMBeanTest:cache-name=test(local),jmx-resource=MvccLockManager");

      threadMBeanServer = PerThreadMBeanServerLookup.getThreadMBeanServer();
      return cacheManager;
   }

   public void testConcurrencyLevel() throws Exception {
      assertAttributeValue("ConcurrencyLevel", CONCURRENCY_LEVEL);
   }

   public void testNumberOfLocksHeld() throws Exception {
      TransactionManager tm = TestingUtil.extractComponent(cache, TransactionManager.class);
      tm.begin();
      cache.put("key", "value");
      assertAttributeValue("NumberOfLocksHeld", 1);
      cache.put("key2", "value2");
      assertAttributeValue("NumberOfLocksHeld", 2);
      tm.commit();
      assertAttributeValue("NumberOfLocksHeld", 0);
   }

   public void testNumberOfLocksAvailable() throws Exception {
      TransactionManager tm = TestingUtil.extractComponent(cache, TransactionManager.class);
      int initialAvailable = getAttrValue("NumberOfLocksAvailable");
      tm.begin();
      cache.put("key", "value");
      assertAttributeValue("NumberOfLocksHeld", 1);
      assertAttributeValue("NumberOfLocksAvailable", initialAvailable - 1);
      tm.rollback();
      assertAttributeValue("NumberOfLocksAvailable", initialAvailable);
      assertAttributeValue("NumberOfLocksHeld", 0);
   }

   private void assertAttributeValue(String attrName, int expectedVal) throws Exception {
      int cl = getAttrValue(attrName);
      assert cl == expectedVal : "expected " + expectedVal + ", but received " + cl;
   }

   private int getAttrValue(String attrName) throws Exception {
      return Integer.parseInt(threadMBeanServer.getAttribute(lockManagerObjName, attrName).toString());
   }
}
