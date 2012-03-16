/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.api.mvcc;

import org.infinispan.Cache;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.concurrent.locks.containers.LockContainer;

/**
 * Helper class to assert lock status in MVCC
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 */
public class LockAssert {
   public static void assertLocked(Object key, LockManager lockManager, InvocationContextContainer icc) {
      assert lockManager.isLocked(key) : key + " not locked!";
   }

   public static void assertNotLocked(Object key, InvocationContextContainer icc) {
      // can't rely on the negative test since other entries may share the same lock with lock striping.
      assert !icc.createInvocationContext(true, -1).hasLockedKey(key) : key + " lock recorded!";
   }

   public static void assertNoLocks(LockManager lockManager, InvocationContextContainer icc) {
      LockContainer lc = (LockContainer) TestingUtil.extractField(lockManager, "lockContainer");
      assert lc.getNumLocksHeld() == 0 : "Stale locks exist! NumLocksHeld is " + lc.getNumLocksHeld() + " and lock info is " + lockManager.printLockInfo();
   }

   public static void assertNoLocks(Cache cache) {
      LockManager lockManager = TestingUtil.extractComponentRegistry(cache).getComponent(LockManager.class);
      InvocationContextContainer icc = TestingUtil.extractComponentRegistry(cache).getComponent(InvocationContextContainer.class);

      assertNoLocks(lockManager, icc);
   }
}
