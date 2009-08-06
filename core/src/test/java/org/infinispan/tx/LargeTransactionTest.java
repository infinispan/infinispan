package org.infinispan.tx;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.manager.CacheManager;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import javax.transaction.TransactionManager;

/**
 *  Test for: https://jira.jboss.org/jira/browse/ISPN-149.
 *
 * @author Mircea.Markus@jboss.com
 */
@Test(testName = "tx.LargeTransactionTest", groups = "functional")
public class LargeTransactionTest extends MultipleCacheManagersTest {
   private Cache cache;

   protected void createCacheManagers() throws Throwable {

      Configuration c = new Configuration();
      c.setInvocationBatchingEnabled(true);
      c.setCacheMode(Configuration.CacheMode.REPL_SYNC);
      c.setSyncReplTimeout(30000);
      c.setLockAcquisitionTimeout(60000);
      c.setUseLockStriping(false);

      CacheManager manager = new DefaultCacheManager(TestCacheManagerFactory.getGlobalConfigurtion(), c);
      registerCacheManager(manager);
      cache = manager.getCache("TestCache");

      manager = new DefaultCacheManager(TestCacheManagerFactory.getGlobalConfigurtion(), c);
      registerCacheManager(manager);
      manager.getCache("TestCache");
   }

   @Override
   protected void assertSupportedConfig() {
      //yep!
   }

   public void simpleTest() throws Exception {
      TransactionManager tm = TestingUtil.getTransactionManager(cache);
      tm.begin();
      for (int i = 0; i < 200; i++)
         cache.put("key" + i, "value" + i);
      tm.commit();

      Cache cache2 = cache(1, "TestCache");
      for (int i = 0; i < 200; i++) {
         assert cache2.get("key" + i).equals("value+i");
      }

   }
}
