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

package org.infinispan.lock;

import org.infinispan.affinity.KeyAffinityService;
import org.infinispan.affinity.KeyAffinityServiceFactory;
import org.infinispan.affinity.RndKeyGenerator;
import org.infinispan.config.Configuration;
import org.infinispan.distribution.rehash.XAResourceAdapter;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;

/**
 * Tester for https://jira.jboss.org/browse/ISPN-615.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
@Test(groups = "functional", testName = "lock.EagerLockingSingleLockTest")
public class EagerLockingSingleLockTest extends MultipleCacheManagersTest {
   private KeyAffinityService kaf;
   private ThreadPoolExecutor poolExecutor;

   private static final Log log = LogFactory.getLog(EagerLockingSingleLockTest.class);

   @Override
   protected void createCacheManagers() throws Throwable {
      Configuration config = getDefaultClusteredConfig(Configuration.CacheMode.DIST_SYNC, true);
      config.setEagerLockSingleNode(true);
      config.setNumOwners(2);
      config.setLockAcquisitionTimeout(2000);
      config.setUseEagerLocking(true);
      config.setL1CacheEnabled(false);
      createClusteredCaches(4, config);
      poolExecutor = new ThreadPoolExecutor(1, 1, 1000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(10));
      kaf = KeyAffinityServiceFactory.newKeyAffinityService(cache(0), poolExecutor, new RndKeyGenerator(), 10, true);
   }

   @AfterClass
   public void cleanUp() {
      kaf.stop();
      poolExecutor.shutdownNow();
   }

   public void testSingleLockAcquiredRemotely() throws Exception {
      log.trace("0 -> " + address(0));
      log.trace("1 -> " + address(1));
      log.trace("2 -> " + address(2));
      log.trace("3 -> " + address(3));

      Object k = kaf.getKeyForAddress(address(3));
      cache(1).put(k, "1stValue");
      TransactionManager tm = cache(0).getAdvancedCache().getTransactionManager();
      tm.begin();
      cache(1).put(k, "2ndValue");
      assert !TestingUtil.extractLockManager(cache(0)).isLocked(k);
      assert TestingUtil.extractLockManager(cache(1)).isLocked(k);
      assert !TestingUtil.extractLockManager(cache(2)).isLocked(k);
      assert TestingUtil.extractLockManager(cache(3)).isLocked(k);

      Transaction lockOwnerTx = tm.suspend();

      //now make sure other transactions won't be able to acquire locks
      tm.begin();
      try {
         cache(2).put(k, "3rdValue");
         assert false;
      } catch (TimeoutException e) {
         assertEquals(tm.getStatus(), Status.STATUS_MARKED_ROLLBACK);
      }
      tm.rollback();

      //now check that the only node that has a remote lock is cache(3)
      assert !TestingUtil.extractLockManager(cache(0)).isLocked(k);
      assert !TestingUtil.extractLockManager(cache(2)).isLocked(k);
      assert TestingUtil.extractLockManager(cache(3)).isLocked(k);

      tm.resume(lockOwnerTx);
      tm.commit();

      //no locks are being held now
      assert !TestingUtil.extractLockManager(cache(0)).isLocked(k);
      assert !TestingUtil.extractLockManager(cache(1)).isLocked(k);
      assert !TestingUtil.extractLockManager(cache(2)).isLocked(k);
      assert !TestingUtil.extractLockManager(cache(3)).isLocked(k);

      assertEquals(cache(0).get(k), "2ndValue");
   }

   @Test(dependsOnMethods = "testSingleLockAcquiredRemotely")
   public void testSingleLockAcquiredLocally() throws Exception {
      Object k = kaf.getKeyForAddress(address(1));
      cache(1).put(k, "1stValue");
      TransactionManager tm = cache(0).getAdvancedCache().getTransactionManager();





      tm.begin();
      cache(1).put(k, "2ndValue");//this acquires a local cache only

      //now check that the only node that has a remote lock is cache(3)
      assert !TestingUtil.extractLockManager(cache(0)).isLocked(k);
      assert TestingUtil.extractLockManager(cache(1)).isLocked(k);
      assert !TestingUtil.extractLockManager(cache(2)).isLocked(k);
      assert !TestingUtil.extractLockManager(cache(3)).isLocked(k);

      tm.commit();

      //no locks are being held now
      assert !TestingUtil.extractLockManager(cache(0)).isLocked(k);
      assert !TestingUtil.extractLockManager(cache(1)).isLocked(k);
      assert !TestingUtil.extractLockManager(cache(2)).isLocked(k);
      assert !TestingUtil.extractLockManager(cache(3)).isLocked(k);

      assertEquals(cache(0).get(k), "2ndValue");
   }

   @Test(dependsOnMethods = "testSingleLockAcquiredLocally")
   public void testLockOwnerFailure() throws Exception {
      log.info("Start here.");
      Object k = kaf.getKeyForAddress(address(3));
      cache(1).put(k, "1stValue");
      final TransactionManager tm = cache(0).getAdvancedCache().getTransactionManager();

      tm.begin();
      cache(1).put(k, "2ndValue");
      tm.getTransaction().enlistResource(new XAResourceAdapter());

      //now check that the only node that has a remote lock is cache(3)
      assert !TestingUtil.extractLockManager(cache(0)).isLocked(k);
      assert !TestingUtil.extractLockManager(cache(2)).isLocked(k);
      assert TestingUtil.extractLockManager(cache(3)).isLocked(k);

      manager(3).stop();
      TestingUtil.blockUntilViewsReceived(10000, false, cache(0), cache(1), cache(2));
      TestingUtil.waitForRehashToComplete(cache(0), cache(1), cache(2));

      try {
         log.trace("here it begins");
         tm.commit();
         assert false;
      } catch (RollbackException re) {
         //expected
      }

      //no locks are being held now
      assert !TestingUtil.extractLockManager(cache(0)).isLocked(k);
      assert !TestingUtil.extractLockManager(cache(1)).isLocked(k);
      assert !TestingUtil.extractLockManager(cache(2)).isLocked(k);

      assertEquals(cache(0).get(k), "1stValue");
   }
}
