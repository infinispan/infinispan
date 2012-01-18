/*
 * Copyright 2011 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */

package org.infinispan.util.concurrent.locks.containers;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

public abstract class AbstractLockContainer<L extends Lock> implements LockContainer<L> {

   /**
    * Releases a lock and swallows any IllegalMonitorStateExceptions - so it is safe to call this method even if the
    * lock is not locked, or not locked by the current thread.
    *
    * @param toRelease lock to release
    */
   protected void safeRelease(L toRelease, Object lockOwner) {
      if (toRelease != null) {
         try {
            unlock(toRelease, lockOwner);
         } catch (IllegalMonitorStateException imse) {
            // Perhaps the caller hadn't acquired the lock after all.
         }
      }
   }

   protected abstract void unlock(L toRelease, Object ctx);

   protected abstract boolean tryLock(L lock, long timeout, TimeUnit unit, Object lockOwner) throws InterruptedException;
}
