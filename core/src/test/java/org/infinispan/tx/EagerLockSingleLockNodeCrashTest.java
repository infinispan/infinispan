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

import org.infinispan.config.Configuration;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.LockingMode;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;

/**
 * Tester for https://issues.jboss.org/browse/ISPN-1074.
 * @author Mircea Markus
 * @since 4.2
 */
@Test (groups = "functional", testName = "tx.EagerLockSingleLockNodeCrashTest")
@CleanupAfterMethod
public class EagerLockSingleLockNodeCrashTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      Configuration config = getDefaultClusteredConfig(Configuration.CacheMode.DIST_SYNC, true);
      config.fluent().hash().numOwners(3).transaction().useEagerLocking(true).eagerLockSingleNode(true);
      config.fluent().transaction().lockingMode(LockingMode.PESSIMISTIC);
      createCluster(config, 3);
      waitForClusterToForm();
   }

   public void testMainOwnerRemoteFailure() throws Exception {
      tm(0).begin();
      Object key1 = getKeyForCache(2);
      cache(0).put(key1, "value");
      for (int i = 0; i < 3; i++) {
         log.tracef("i = %s, owner = %s, isLocked = %s", i, 2, lockManager(i).isLocked(key1));
         assertEquals(lockManager(i).isLocked(key1), 2 == i || i == 0);
      }
      killMember(2);
      assert tm(0).getTransaction() != null;
      try {
         tm(0).commit();
         assert false;
      } catch (Exception e) {
         e.printStackTrace();
         //expected
      }
      assert !lockManager(0).isLocked(key1);
      assert !lockManager(1).isLocked(key1);
   }
}
