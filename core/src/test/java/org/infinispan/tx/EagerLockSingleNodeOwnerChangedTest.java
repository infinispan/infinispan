/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
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

import org.infinispan.affinity.KeyAffinityService;
import org.infinispan.affinity.KeyAffinityServiceFactory;
import org.infinispan.affinity.RndKeyGenerator;
import org.infinispan.config.Configuration;
import org.infinispan.distribution.rehash.XAResourceAdapter;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import javax.transaction.InvalidTransactionException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Test for https://issues.jboss.org/browse/ISPN-1007.
 *
 * @author Mircea Markus
 * @since 5.0
 */
@Test (groups = "functional", testName = "tx.EagerLockSingleNodeOwnerChangedTest")
@CleanupAfterMethod
public class EagerLockSingleNodeOwnerChangedTest extends MultipleCacheManagersTest {

   private ExecutorService ex;
   private Configuration c;
   private KeyAffinityService kaf;

   @Override
   protected void createCacheManagers() throws Throwable {
      c = getDefaultClusteredConfig(Configuration.CacheMode.DIST_SYNC);
      c.fluent()
            .transaction().useEagerLocking(true).eagerLockSingleNode(true)
            .clustering().hash().numOwners(3);
      createCluster(c, 2);
      waitForClusterToForm();
      ex = Executors.newSingleThreadExecutor();
      kaf = KeyAffinityServiceFactory.newKeyAffinityService(this.cache(0), ex, new RndKeyGenerator(), 100, true);
   }

   @AfterMethod
   public void tearDown() {
      kaf.stop();
      ex.shutdown();
   }

   public void testLocalKeyOwnerChanged() throws Exception {
      //generate 100 transactions
      Map<Object, Transaction> transactions = new HashMap<Object, Transaction>();
      for (int i = 0; i < 100; i++) {
         Object key = kaf.getKeyForAddress(address(0));
         if (transactions.containsKey(key)) continue;
         cache(0).put(key, oldValue(key));

         tm(0).begin();
         cache(0).put(key, key);
         Transaction suspend = tm(0).suspend();
         assert suspend != null;
         transactions.put(key, suspend);

      }

      addNewClusterMember();


      log.info("About to..");

      for (Object o : transactions.keySet()) {
         List<Address> owners = advancedCache(2).getDistributionManager().getConsistentHash().locate(o, 2);
         boolean mainOwnerChanged = owners.get(0).equals(address(2));
         if (mainOwnerChanged) {
            Transaction t = transactions.get(o);
            testCommitFailsAndOldValues(o, t);
            return;
         }
      }
   }

   public void testNotLocalKeyChanged() throws Exception {
      Set<Object> keys = new HashSet<Object>();
      for (int i = 0; i < 100; i++) {
         Object keyForAddress = kaf.getKeyForAddress(address(1));
         if (keys.add(keyForAddress)) {
            cache(0).put(keyForAddress, oldValue(keyForAddress));
         }
      }
      tm(0).begin();
      for (Object o : keys) {
         cache(0).put(o, o);
      }
      Transaction suspend = tm(0).suspend();

      addNewClusterMember();

      for (Object o : keys) {
         List<Address> owners = advancedCache(2).getDistributionManager().getConsistentHash().locate(o, 2);
         boolean mainOwnerChanged = owners.get(0).equals(address(2));
         if (mainOwnerChanged) {
            System.out.println("Found local! " + o);
            log.infof("Found local! %s", o);
            testCommitFailsAndOldValues(o, suspend);
            return;
         }
      }

   }

   private String oldValue(Object key) {
      return key + "oldValue";
   }

   private void testCommitFailsAndOldValues(Object o, Transaction t) throws InvalidTransactionException, SystemException, RollbackException {
      System.out.println("Found local! " + o);
      log.infof("Found local! %s", o);
      System.out.println("t = " + t);
      tm(0).resume(t);
      t.enlistResource(new XAResourceAdapter());
      try {
         tm(0).commit();
         assert false;
         //fail
      } catch (Exception e) {
         e.printStackTrace();
         //expected
      }

      assert cache(0).get(o).equals(oldValue(o));
      assert cache(1).get(o).equals(oldValue(o));
      assert cache(2).get(o).equals(oldValue(o));
      assert !lockManager(0).isLocked(o);
      assert !lockManager(1).isLocked(o);
      assert !lockManager(2).isLocked(o);
   }

   private void addNewClusterMember() {
      //now add the new cache
      addClusterEnabledCacheManager(c);
      waitForClusterToForm();
   }
}
