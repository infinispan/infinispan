package org.infinispan.tx;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

import javax.transaction.Status;
import javax.transaction.TransactionManager;

import static org.infinispan.test.TestingUtil.sleepThread;

@Test (groups = "unstable", testName = "tx.RemoteLockCleanupStressTest", invocationCount = 20, description = "original group: functional")
@CleanupAfterMethod
public class RemoteLockCleanupStressTest extends MultipleCacheManagersTest {

   private static final Log log = LogFactory.getLog(RemoteLockCleanupStressTest.class);

   private String key = "locked-counter";

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder c = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
      c.clustering().stateTransfer().fetchInMemoryState(true)
            .locking().lockAcquisitionTimeout(1500);

      createClusteredCaches(2, c);
   }

   public void testLockRelease() {
      final EmbeddedCacheManager cm1 = manager(0);
      final EmbeddedCacheManager cm2 = manager(1);
      Thread t1 = new Thread(new CounterTask(cm1));
      Thread t2 = new Thread(new CounterTask(cm2));

      t1.start();
      t2.start();

      sleepThread(1000);
      t2.interrupt();
      TestingUtil.killCacheManagers(cm2);
      cacheManagers.remove(1);
      sleepThread(1100);
      t1.interrupt();
      LockManager lm = TestingUtil.extractComponent(cm1.getCache(), LockManager.class);
      Object owner = lm.getOwner(key);
      assert ownerIsLocalOrUnlocked(owner, cm1.getAddress()) : "Bad lock owner " + owner;
   }

   private boolean ownerIsLocalOrUnlocked(Object owner, Address self) {
      if (owner == null) return true;
      if (owner instanceof GlobalTransaction) {
         GlobalTransaction gtx = ((GlobalTransaction) owner);
         return gtx.getAddress().equals(self);
      } else {
         return false;
      }
   }

   class CounterTask implements Runnable {
      EmbeddedCacheManager cm;

      CounterTask(EmbeddedCacheManager cm) {
         this.cm = cm;
      }

      @Override
      public void run() {
         for (int i=0; i<25; i++) run_();
      }

      public void run_() {
         Cache cache = cm.getCache();
         TransactionManager tx = cache.getAdvancedCache().getTransactionManager();
         try {
            tx.begin();
         } catch (Exception ex) {
            log.debug("Exception starting transaction", ex);
         }

         try {
            log.debug("aquiring lock on cache " + cache.getName() + " key " + key + "...");
            cache.getAdvancedCache().lock(key);

            Integer val = (Integer) cache.get(key);
            log.debug("current value : " + val);
            if (val == null) {
               val = 0;
            } else {
               val++;

            }
            cache.put(key, val);
            TestingUtil.sleepRandom(200);

            log.debug("commit...");
            tx.commit();
            log.debug("done commit");
         } catch (Exception ex) {
            try {
               log.debug("rollback... " + ex.getLocalizedMessage());
               tx.rollback();
               log.debug("done rollback");
            } catch (Exception rex) {
               log.debug("Exception rolling back", rex);
            }
         } finally {
            try {
               log.debug("tx status at the end : ");
               switch (tx.getStatus()) {
                  case Status.STATUS_ACTIVE:
                     log.debug("active");
                     break;
                  case Status.STATUS_COMMITTED:
                     log.debug("committed");
                     break;
                  case Status.STATUS_COMMITTING:
                     log.debug("committing");
                     break;
                  case Status.STATUS_MARKED_ROLLBACK:
                     log.debug("makerd rollback");
                     break;
                  case Status.STATUS_NO_TRANSACTION:
                     log.debug("no transaction");
                     break;
                  case Status.STATUS_PREPARED:
                     log.debug("preprared");
                     break;
                  case Status.STATUS_PREPARING:
                     log.debug("preparing");
                     break;
                  case Status.STATUS_ROLLEDBACK:
                     log.debug("rolledback");
                     break;
                  case Status.STATUS_ROLLING_BACK:
                     log.debug("rolling back");
                     break;
                  case Status.STATUS_UNKNOWN:
                     log.debug("unknown");
                     break;
                  default:
                     log.debug(tx.getStatus());
               }
            } catch (Exception ex) {
               log.debug("Exception retrieving transaction status", ex);
            }
         }
      }
   }
}
