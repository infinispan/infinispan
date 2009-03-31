package org.horizon.jmx;

import org.horizon.Cache;
import org.horizon.config.Configuration;
import org.horizon.config.GlobalConfiguration;
import org.horizon.manager.CacheManager;
import org.horizon.test.MultipleCacheManagersTest;
import org.horizon.test.TestingUtil;
import org.horizon.test.fwk.TestCacheManagerFactory;
import org.horizon.transaction.DummyTransactionManagerLookup;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.transaction.TransactionManager;

/**
 * Tester class for {@link TxInterceptor} jmx stuff.
 *
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "jmx.TxInterceptorMBeanTest")
public class TxInterceptorMBeanTest extends MultipleCacheManagersTest {

   private ObjectName txInterceptor;
   private MBeanServer threadMBeanServer;
   private TransactionManager tm;
   private Cache cache1;
   private Cache cache2;

   protected void createCacheManagers() throws Throwable {
      GlobalConfiguration globalConfiguration = GlobalConfiguration.getClusteredDefault();
      globalConfiguration.setExposeGlobalJmxStatistics(true);
      globalConfiguration.setAllowDuplicateDomains(true);
      globalConfiguration.setMBeanServerLookup(PerThreadMBeanServerLookup.class.getName());
      globalConfiguration.setJmxDomain("TxInterceptorMBeanTest");
      CacheManager cacheManager1 = TestCacheManagerFactory.createCacheManager(globalConfiguration);
      registerCacheManager(cacheManager1);
      CacheManager cacheManager2 = TestCacheManagerFactory.createCacheManager(globalConfiguration.clone());
      registerCacheManager(cacheManager2);

      Configuration configuration = getDefaultClusteredConfig(Configuration.CacheMode.REPL_SYNC);
      configuration.setExposeJmxStatistics(true);
      configuration.setTransactionManagerLookupClass(DummyTransactionManagerLookup.class.getName());
      cacheManager1.defineCache("test", configuration);
      cacheManager2.defineCache("test", configuration.clone());
      cache1 = cacheManager1.getCache("test");
      cache2 = cacheManager2.getCache("test");
      txInterceptor = new ObjectName("TxInterceptorMBeanTest:cache-name=test(repl_sync),jmx-resource=TxInterceptor");

      threadMBeanServer = PerThreadMBeanServerLookup.getThreadMBeanServer();
      tm = TestingUtil.getTransactionManager(cache1);
   }

   @AfterMethod
   public void resetStats() throws Exception {
      threadMBeanServer.invoke(txInterceptor, "resetStatistics", new Object[0], new String[0]);
   }


   public void testCommit() throws Exception {
      assertCommitRollback(0, 0);
      tm.begin();
      assertCommitRollback(0, 0);
      cache1.put("key", "value");
      assertCommitRollback(0, 0);
      tm.commit();
      assertCommitRollback(1, 0);
   }
   
   public void testRollback() throws Exception {
      assertCommitRollback(0, 0);
      tm.begin();
      assertCommitRollback(0, 0);
      cache1.put("key", "value");
      assertCommitRollback(0, 0);
      tm.rollback();
      assertCommitRollback(0, 1);
   }

   public void testRemoteCommit() throws Exception {
      assertCommitRollback(0, 0);
      tm.begin();
      assertCommitRollback(0, 0);
      cache2.put("key", "value");
      assertCommitRollback(0, 0);
      tm.commit();
      assertCommitRollback(1, 0);
   }

   public void testRemoteRollback() throws Exception {
      assertCommitRollback(0, 0);
      tm.begin();
      assertCommitRollback(0, 0);
      cache2.put("key", "value");
      assertCommitRollback(0, 0);
      tm.rollback();
      assertCommitRollback(0, 1);
   }

   private void assertCommitRollback(int commit, int rollback) throws Exception {
      String commitCount = threadMBeanServer.getAttribute(txInterceptor, "Commits").toString();
      assert Integer.valueOf(commitCount) == commit : "expecting " + commit + " commits, received " + commitCount;
      String rollbackCount = threadMBeanServer.getAttribute(txInterceptor, "Rollbacks").toString();
      assert Integer.valueOf(commitCount) == commit : "expecting " + rollback + " rollbacks, received " + rollbackCount;
   }
}
