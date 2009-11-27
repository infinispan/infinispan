package org.infinispan.tx;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

import javax.transaction.NotSupportedException;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;
import java.util.List;

/**
 * Tests what happens when a member acquires locks and then dies.
 *
 * @author Manik Surtani
 * @since 4.0
 */
@Test(groups = "functional", testName = "tx.StaleLockRecoveryTest")
public class StaleLockRecoveryTest extends MultipleCacheManagersTest {
   Cache<String, String> c1, c2;

   @Override
   protected void createCacheManagers() throws Throwable {
      Configuration c = getDefaultClusteredConfig(Configuration.CacheMode.REPL_SYNC, true);
      c.setLockAcquisitionTimeout(500);
      List<Cache<String, String>> caches = createClusteredCaches(2, "tx", c);
      c1 = caches.get(0);
      c2 = caches.get(1);
   }

   public void testStaleLock() throws SystemException, NotSupportedException {
      c1.put("k", "v");
      assert c1.get("k").equals("v");
      assert c2.get("k").equals("v");

      TransactionManager tm = TestingUtil.getTransactionManager(c1);
      tm.begin();
      c1.getAdvancedCache().lock("k");
      tm.suspend();

      // test that both c1 and c2 have locked k
      assertLocked(c1, "k");
      assertLocked(c2, "k");

      cacheManagers.get(0).stop();

      assert c2.getCacheManager().getMembers().size() == 1;

      // may take a while from when the view change is seen through to when the lock is cleared
      TestingUtil.sleepThread(1000);

      assertNotLocked(c2, "k");
   }

   private void assertLocked(Cache<String, String> c, String key) throws SystemException, NotSupportedException {
      TransactionManager tm = TestingUtil.getTransactionManager(c);
      tm.begin();
      try {
         c.put(key, "dummy"); // should time out
         assert false : "Should have been locked!";
      } catch (Exception e) {

      } finally {
         tm.rollback();
      }
   }

   private void assertNotLocked(Cache<String, String> c, String key) throws SystemException, NotSupportedException {
      TransactionManager tm = TestingUtil.getTransactionManager(c);
      tm.begin();
      try {
         c.put(key, "dummy"); // should time out
      } catch (Exception e) {
         assert false : "Should not have been locked!";
      } finally {
         tm.rollback();
      }
   }
}
