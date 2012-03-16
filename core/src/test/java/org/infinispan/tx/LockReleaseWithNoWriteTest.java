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
import org.infinispan.transaction.LockingMode;
import org.testng.annotations.Test;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;

/**
 * @author Mircea Markus <mircea.markus@jboss.com> (C) 2011 Red Hat Inc.
 * @since 5.1
 */
@Test (groups = "functional", testName = "tx.LockReleaseWithNoWriteTest")
public class LockReleaseWithNoWriteTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      Configuration dcc = getDefaultClusteredConfig(Configuration.CacheMode.DIST_SYNC, true);
      dcc.fluent().transaction().lockingMode(LockingMode.PESSIMISTIC).syncCommitPhase(false);
      createCluster(dcc, 2);
      waitForClusterToForm();
   }

   public void testLocksReleased1() throws Exception {
      runtTest(1, 0);
   }
   public void testLocksReleased2() throws Exception {
      runtTest(1, 1);
   }
   public void testLocksReleased3() throws Exception {
      runtTest(0, 0);
   }
   public void testLocksReleased4() throws Exception {
      runtTest(0, 1);
   }

   private void runtTest(int lockOwner, int txOwner) throws NotSupportedException, SystemException, RollbackException, HeuristicMixedException, HeuristicRollbackException {
      Object key = getKeyForCache(lockOwner);
      tm(txOwner).begin();
      advancedCache(txOwner).lock(key);
      tm(txOwner).commit();

      assertNotLocked(key);
   }
}
