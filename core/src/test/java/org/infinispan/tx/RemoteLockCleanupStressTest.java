/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
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

@Test (groups = "functional", testName = "tx.RemoteLockCleanupStressTest", invocationCount = 20, enabled = false)
@CleanupAfterMethod
public class RemoteLockCleanupStressTest extends MultipleCacheManagersTest {

   private static final Log log = LogFactory.getLog(RemoteLockCleanupStressTest.class);

   private EmbeddedCacheManager cm1, cm2;
   private String key = "locked-counter";

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder c = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
      c.clustering().stateTransfer().fetchInMemoryState(true)
            .locking().lockAcquisitionTimeout(1500);

      cm1 = addClusterEnabledCacheManager(c);
      cm2 = addClusterEnabledCacheManager(c);
   }

   public void testLockRelease() {

      Thread t1 = new Thread(new CounterTask(cm1));
      Thread t2 = new Thread(new CounterTask(cm2));

      t1.start();
      t2.start();

      sleepThread(1000);
      t2.interrupt();
      TestingUtil.killCacheManagers(cm2);
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