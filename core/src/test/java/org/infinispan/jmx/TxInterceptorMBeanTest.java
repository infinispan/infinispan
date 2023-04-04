package org.infinispan.jmx;

import static org.infinispan.test.TestingUtil.getCacheObjectName;
import static org.infinispan.test.fwk.TestCacheManagerFactory.configureJmx;
import static org.testng.AssertJUnit.assertEquals;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import jakarta.transaction.TransactionManager;

import org.infinispan.Cache;
import org.infinispan.commons.jmx.MBeanServerLookup;
import org.infinispan.commons.jmx.TestMBeanServerLookup;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.distribution.rehash.XAResourceAdapter;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "jmx.TxInterceptorMBeanTest")
public class TxInterceptorMBeanTest extends MultipleCacheManagersTest {

   private static final String JMX_DOMAIN = TxInterceptorMBeanTest.class.getSimpleName();

   private final MBeanServerLookup mBeanServerLookup = TestMBeanServerLookup.create();
   private ObjectName txInterceptor;
   private ObjectName txInterceptor2;
   private TransactionManager tm;
   private Cache<String, String> cache1;
   private Cache<String, String> cache2;

   @Override
   protected void createCacheManagers() throws Throwable {
      GlobalConfigurationBuilder globalBuilder = GlobalConfigurationBuilder.defaultClusteredBuilder();
      configureJmx(globalBuilder, JMX_DOMAIN, mBeanServerLookup);
      EmbeddedCacheManager cacheManager1 = TestCacheManagerFactory.createClusteredCacheManager(globalBuilder, new ConfigurationBuilder());
      registerCacheManager(cacheManager1);

      GlobalConfigurationBuilder globalBuilder2 = GlobalConfigurationBuilder.defaultClusteredBuilder();
      globalBuilder2.cacheManagerName("SecondDefaultCacheManager");
      configureJmx(globalBuilder2, JMX_DOMAIN + 2, mBeanServerLookup);
      EmbeddedCacheManager cacheManager2 = TestCacheManagerFactory.createClusteredCacheManager(globalBuilder2, new ConfigurationBuilder());
      registerCacheManager(cacheManager2);

      ConfigurationBuilder configuration = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
      configuration.statistics().enable();
      cacheManager1.defineConfiguration("test", configuration.build());
      cacheManager2.defineConfiguration("test", configuration.build());

      cache1 = cacheManager1.getCache("test");
      cache2 = cacheManager2.getCache("test");
      txInterceptor = getCacheObjectName(JMX_DOMAIN, "test(repl_sync)", "Transactions");
      txInterceptor2 = getCacheObjectName(JMX_DOMAIN + 2, "test(repl_sync)", "Transactions", "SecondDefaultCacheManager");

      tm = TestingUtil.getTransactionManager(cache1);
   }

   @AfterMethod
   public void resetStats() throws Exception {
      TestingUtil.getMBeanServer(cache1).invoke(txInterceptor, "resetStatistics", new Object[0], new String[0]);
      TestingUtil.getMBeanServer(cache2).invoke(txInterceptor2, "resetStatistics", new Object[0], new String[0]);
   }

   public void testJmxOperationMetadata() throws Exception {
      TestingUtil.checkMBeanOperationParameterNaming(TestingUtil.getMBeanServer(cache1), txInterceptor);
   }

   public void testCommit() throws Exception {
      assertCommitRollback(0, 0, cache1, txInterceptor);
      tm.begin();
      //enlist another resource adapter to force TM to execute 2PC (otherwise 1PC)
      tm.getTransaction().enlistResource(new XAResourceAdapter());
      assertCommitRollback(0, 0, cache1, txInterceptor);
      cache1.put("key", "value");
      assertCommitRollback(0, 0, cache1, txInterceptor);
      tm.commit();
      assertCommitRollback(1, 0, cache1, txInterceptor);
   }

   public void testRollback() throws Exception {
      assertCommitRollback(0, 0, cache1, txInterceptor);
      tm.begin();
      assertCommitRollback(0, 0, cache1, txInterceptor);
      cache1.put("key", "value");
      assertCommitRollback(0, 0, cache1, txInterceptor);
      tm.rollback();
      assertCommitRollback(0, 1, cache1, txInterceptor);
   }

   public void testRemoteCommit() throws Exception {
      assertCommitRollback(0, 0, cache2, txInterceptor2);
      tm.begin();
      assertCommitRollback(0, 0, cache2, txInterceptor2);
      //enlist another resource adapter to force TM to execute 2PC (otherwise 1PC)
      tm.getTransaction().enlistResource(new XAResourceAdapter());
      cache2.put("key", "value");
      assertCommitRollback(0, 0, cache2, txInterceptor2);
      tm.commit();
      assertCommitRollback(1, 0, cache2, txInterceptor2);
   }

   public void testRemoteRollback() throws Exception {
      assertCommitRollback(0, 0, cache2, txInterceptor2);
      tm.begin();
      assertCommitRollback(0, 0, cache2, txInterceptor2);
      cache2.put("key", "value");
      assertCommitRollback(0, 0, cache2, txInterceptor2);
      tm.rollback();
      assertCommitRollback(0, 1, cache2, txInterceptor2);
   }

   private void assertCommitRollback(int commit, int rollback, Cache<String, String> cache, ObjectName objectName) throws Exception {
      MBeanServer mBeanServer = TestingUtil.getMBeanServer(cache);

      Long commitCount = (Long) mBeanServer.getAttribute(objectName, "Commits");
      assertEquals("expecting " + commit + " commits, received " + commitCount, commit, commitCount.intValue());

      Long rollbackCount = (Long) mBeanServer.getAttribute(objectName, "Rollbacks");
      assertEquals("expecting " + rollback + " rollbacks, received " + rollbackCount, rollback, rollbackCount.intValue());
   }
}
