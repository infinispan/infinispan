package org.infinispan.jmx;

import org.infinispan.config.Configuration;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.manager.CacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.transaction.TransactionManager;

/**
 * Test the JMX functionality in {@link org.infinispan.util.concurrent.locks.LockManagerImpl}.
 *
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "jmx.MvccLockManagerMBeanTest")
public class MvccLockManagerMBeanTest extends SingleCacheManagerTest {
   public static final int CONCURRENCY_LEVEL = 129;

   private ObjectName lockManagerObjName;
   private MBeanServer threadMBeanServer;
   private static final String JMX_DOMAIN = "MvccLockManagerMBeanTest";

   protected CacheManager createCacheManager() throws Exception {
      GlobalConfiguration globalConfiguration = GlobalConfiguration.getNonClusteredDefault();
      globalConfiguration.setExposeGlobalJmxStatistics(true);
      globalConfiguration.setMBeanServerLookup(PerThreadMBeanServerLookup.class.getName());
      globalConfiguration.setJmxDomain(JMX_DOMAIN);
      cacheManager = TestCacheManagerFactory.createCacheManagerEnforceJmxDomain(globalConfiguration);

      Configuration configuration = getDefaultStandaloneConfig(true);
      configuration.setExposeJmxStatistics(true);
      configuration.setConcurrencyLevel(CONCURRENCY_LEVEL);

      cacheManager.defineConfiguration("test", configuration);
      cache = cacheManager.getCache("test");
      lockManagerObjName = new ObjectName(JMX_DOMAIN + ":cache-name=test(local),jmx-resource=LockManager");

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
