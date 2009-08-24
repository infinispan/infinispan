package org.infinispan.tx;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.manager.CacheManager;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TransactionSetup;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import javax.transaction.TransactionManager;
import java.util.ArrayList;
import java.util.List;

@Test(groups = "functional", testName = "tx.OnePhaseXATest", description = "See ISPN-156 for details.", enabled = false)
public class OnePhaseXATest {
   private List<Cache> caches;
   public static final int CACHES_NUM = 2;

   @Test(invocationCount = 100, skipFailedInvocations = true)
   public void testMultipleCaches() throws Exception {

      //add something  to cache
      int i = 0;
      for (Cache c : caches) {
         TransactionManager tm = TestingUtil.getTransactionManager(c);
         tm.begin();
         c.put("key" + i, "value");
         tm.commit();
         i++;
      }

      //check if caches contain these same keys
      i = 0;
      for (Cache c : caches) {
         assert "value".equals(c.get("key0")) : "Failed getting value for key0 on cache " + i;
         assert "value".equals(c.get("key1")) : "Failed getting value for key1 on cache " + i;
         i++;
      }
   }

   @BeforeTest
   public void setUp() throws Exception {
      caches = new ArrayList<Cache>();
      for (int i = 0; i < CACHES_NUM; i++) caches.add(getCache());
   }

   @AfterTest
   public void tearDown() {
      if (caches != null) TestingUtil.killCaches(caches);
   }

   private Cache getCache() {
      GlobalConfiguration gc = GlobalConfiguration.getClusteredDefault();

      Configuration c = new Configuration();
      c.setInvocationBatchingEnabled(true);
      c.setCacheMode(Configuration.CacheMode.REPL_SYNC);
      c.setSyncReplTimeout(30000);
      c.setLockAcquisitionTimeout(60000);
      c.setUseLockStriping(false);
      c.setSyncCommitPhase(true);
      c.setTransactionManagerLookupClass(TransactionSetup.getManagerLookup());

      CacheManager manager = new DefaultCacheManager(gc, c);
      return manager.getCache("TestCache");
   }
}