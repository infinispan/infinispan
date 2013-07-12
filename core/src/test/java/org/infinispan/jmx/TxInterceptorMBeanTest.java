package org.infinispan.jmx;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.rehash.XAResourceAdapter;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.transaction.TransactionManager;

import static org.infinispan.test.TestingUtil.checkMBeanOperationParameterNaming;
import static org.infinispan.test.TestingUtil.getCacheObjectName;

@Test(groups = "functional", testName = "jmx.TxInterceptorMBeanTest")
public class TxInterceptorMBeanTest extends MultipleCacheManagersTest {
   private static final String JMX_DOMAIN = TxInterceptorMBeanTest.class.getSimpleName();

   private ObjectName txInterceptor;
   private ObjectName txInterceptor2;
   private MBeanServer threadMBeanServer;
   private TransactionManager tm;
   private Cache<String, String> cache1;
   private Cache<String, String> cache2;

   @Override
   protected void createCacheManagers() throws Throwable {
      EmbeddedCacheManager cacheManager1 = TestCacheManagerFactory.createClusteredCacheManagerEnforceJmxDomain(JMX_DOMAIN, true);
      registerCacheManager(cacheManager1);
      EmbeddedCacheManager cacheManager2 = TestCacheManagerFactory.createClusteredCacheManagerEnforceJmxDomain("SecondDefaultCacheManager", JMX_DOMAIN, true);
      registerCacheManager(cacheManager2);

      ConfigurationBuilder configuration = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
      configuration.jmxStatistics().enable();
      cacheManager1.defineConfiguration("test", configuration.build());
      cacheManager2.defineConfiguration("test", configuration.build());
      cache1 = cacheManager1.getCache("test");
      cache2 = cacheManager2.getCache("test");
      txInterceptor = getCacheObjectName(JMX_DOMAIN, "test(repl_sync)", "Transactions");
      txInterceptor2 = getCacheObjectName(JMX_DOMAIN, "test(repl_sync)", "Transactions", "SecondDefaultCacheManager");

      threadMBeanServer = PerThreadMBeanServerLookup.getThreadMBeanServer();
      tm = TestingUtil.getTransactionManager(cache1);
   }

   @AfterMethod
   public void resetStats() throws Exception {
      threadMBeanServer.invoke(txInterceptor, "resetStatistics", new Object[0], new String[0]);
      threadMBeanServer.invoke(txInterceptor2, "resetStatistics", new Object[0], new String[0]);
   }

   public void testJmxOperationMetadata() throws Exception {
      checkMBeanOperationParameterNaming(txInterceptor);
   }

   public void testCommit() throws Exception {
      assertCommitRollback(0, 0, txInterceptor);
      tm.begin();
      //enlist another resource adapter to force TM to execute 2PC (otherwise 1PC)
      tm.getTransaction().enlistResource(new XAResourceAdapter());
      assertCommitRollback(0, 0, txInterceptor);
      cache1.put("key", "value");
      assertCommitRollback(0, 0, txInterceptor);
      tm.commit();
      assertCommitRollback(1, 0, txInterceptor);
   }

   public void testRollback() throws Exception {
      assertCommitRollback(0, 0, txInterceptor);
      tm.begin();
      assertCommitRollback(0, 0, txInterceptor);
      cache1.put("key", "value");
      assertCommitRollback(0, 0, txInterceptor);
      tm.rollback();
      assertCommitRollback(0, 1, txInterceptor);
   }

   public void testRemoteCommit() throws Exception {
      assertCommitRollback(0, 0, txInterceptor2);
      tm.begin();
      assertCommitRollback(0, 0, txInterceptor2);
      //enlist another resource adapter to force TM to execute 2PC (otherwise 1PC)
      tm.getTransaction().enlistResource(new XAResourceAdapter());
      cache2.put("key", "value");
      assertCommitRollback(0, 0, txInterceptor2);
      tm.commit();
      assertCommitRollback(1, 0, txInterceptor2);
   }

   public void testRemoteRollback() throws Exception {
      assertCommitRollback(0, 0, txInterceptor2);
      tm.begin();
      assertCommitRollback(0, 0, txInterceptor2);
      cache2.put("key", "value");
      assertCommitRollback(0, 0, txInterceptor2);
      tm.rollback();
      assertCommitRollback(0, 1, txInterceptor2);
   }

   private void assertCommitRollback(int commit, int rollback, ObjectName objectName) throws Exception {
      String commitCount = threadMBeanServer.getAttribute(objectName, "Commits").toString();
      assert Integer.valueOf(commitCount) == commit : "expecting " + commit + " commits, received " + commitCount;
      String rollbackCount = threadMBeanServer.getAttribute(objectName, "Rollbacks").toString();
      assert Integer.valueOf(commitCount) == commit : "expecting " + rollback + " rollbacks, received " + rollbackCount;
   }
}
