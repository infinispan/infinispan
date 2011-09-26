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
package org.infinispan.util.concurrent.locks.containers;

import net.jcip.annotations.ThreadSafe;
import org.infinispan.context.InvocationContext;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A LockContainer that holds ReentrantLocks
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @see OwnableReentrantStripedLockContainer
 * @since 4.0
 */
@ThreadSafe
public class ReentrantStripedLockContainer extends AbstractStripedLockContainer<ReentrantLock> {
   ReentrantLock[] sharedLocks;

   /**
    * Creates a new LockContainer which uses a certain number of shared locks across all elements that need to be
    * locked.
    *
    * @param concurrencyLevel concurrency level for number of stripes to create.  Stripes are created in powers of two,
    *                         with a minimum of concurrencyLevel created.
    */
   public ReentrantStripedLockContainer(int concurrencyLevel) {
      initLocks(calculateNumberOfSegments(concurrencyLevel));
   }

   protected void initLocks(int numLocks) {
      sharedLocks = new ReentrantLock[numLocks];
      for (int i = 0; i < numLocks; i++) sharedLocks[i] = new ReentrantLock();
   }

   public final ReentrantLock getLock(Object object) {
      return sharedLocks[hashToIndex(object)];
   }

   public final int getNumLocksHeld() {
      int i = 0;
      for (ReentrantLock l : sharedLocks)
         if (l.isLocked()) {
            i++;
         }
      return i;
   }

   public int size() {
      return sharedLocks.length;
   }

   public final boolean ownsLock(Object object, Object ignored) {
      ReentrantLock lock = getLock(object);
      return lock.isHeldByCurrentThread();
   }

   public final boolean isLocked(Object object) {
      ReentrantLock lock = getLock(object);
      return lock.isLocked();
   }

   public String toString() {
      return "ReentrantStripedLockContainer{" +
            "sharedLocks=" + (sharedLocks == null ? null : Arrays.asList(sharedLocks)) +
            '}';
   }

   @Override
   protected void unlock(ReentrantLock l, InvocationContext unused) {
      l.unlock();
   }

   @Override
   protected boolean tryLock(ReentrantLock lock, long timeout, TimeUnit unit, InvocationContext unused) throws InterruptedException {
      return lock.tryLock(timeout, unit);
   }
}
