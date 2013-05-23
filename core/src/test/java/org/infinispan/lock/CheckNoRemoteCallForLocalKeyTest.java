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

package org.infinispan.lock;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.transaction.LockingMode;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * Checks that if a key's lock is the node where the transaction runs, then no remote RPC takes place.
 *
 * @author Mircea Markus
 * @since 5.1
 */
@Test (groups = "functional", testName = "lock.CheckNoRemoteCallForLocalKeyTest")
public class CheckNoRemoteCallForLocalKeyTest extends MultipleCacheManagersTest {

   protected CheckRemoteLockAcquiredOnlyOnceTest.ControlInterceptor controlInterceptor;
   protected CacheMode mode = CacheMode.REPL_SYNC;
   protected Object key = "k";

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder c = getDefaultClusteredCacheConfig(mode, true);
      c.transaction().lockingMode(LockingMode.PESSIMISTIC);
      createCluster(c, 2);
      waitForClusterToForm();
      controlInterceptor = new CheckRemoteLockAcquiredOnlyOnceTest.ControlInterceptor();
      cache(1).getAdvancedCache().addInterceptor(controlInterceptor, 1);
   }

   public void testLocalPut() throws Exception {
      testLocalOperation(new CheckRemoteLockAcquiredOnlyOnceTest.CacheOperation() {
         @Override
         public void execute() {
            cache(0).put(key, "v");
         }
      });
   }

   public void testLocalRemove() throws Exception {
      testLocalOperation(new CheckRemoteLockAcquiredOnlyOnceTest.CacheOperation() {
         @Override
         public void execute() {
            cache(0).remove(key);
         }
      });
   }

   public void testLocalReplace() throws Exception {
      testLocalOperation(new CheckRemoteLockAcquiredOnlyOnceTest.CacheOperation() {
         @Override
         public void execute() {
            cache(0).replace(key, "", "");
         }
      });
   }

   public void testLocalLock() throws Exception {
      testLocalOperation(new CheckRemoteLockAcquiredOnlyOnceTest.CacheOperation() {
         @Override
         public void execute() {
            cache(0).getAdvancedCache().lock(key);
         }
      });
   }

   private void testLocalOperation(CheckRemoteLockAcquiredOnlyOnceTest.CacheOperation o) throws Exception {
      assert !advancedCache(1).getRpcManager().getTransport().isCoordinator();
      assert advancedCache(0).getRpcManager().getTransport().isCoordinator();

      tm(0).begin();

      o.execute();

      assert lockManager(0).isLocked(key);
      assert !lockManager(1).isLocked(key);

      assertEquals(controlInterceptor.remoteInvocations, 0);
      tm(0).rollback();
   }
}
