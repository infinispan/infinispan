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
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.concurrent.locks.OwnableReentrantLock;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * A LockContainer that holds {@link org.infinispan.util.concurrent.locks.OwnableReentrantLock}s.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @see ReentrantStripedLockContainer
 * @see org.infinispan.util.concurrent.locks.OwnableReentrantLock
 * @since 4.0
 */
@ThreadSafe
public class OwnableReentrantStripedLockContainer extends AbstractStripedLockContainer<OwnableReentrantLock> {
   OwnableReentrantLock[] sharedLocks;

   /**
    * Creates a new LockContainer which uses a certain number of shared locks across all elements that need to be
    * locked.
    *
    * @param concurrencyLevel concurrency level for number of stripes to create.  Stripes are created in powers of two,
    *                         with a minimum of concurrencyLevel created.
    */
   public OwnableReentrantStripedLockContainer(int concurrencyLevel) {
      initLocks(calculateNumberOfSegments(concurrencyLevel));
   }

   protected void initLocks(int numLocks) {
      sharedLocks = new OwnableReentrantLock[numLocks];
      for (int i = 0; i < numLocks; i++) sharedLocks[i] = new OwnableReentrantLock();
   }

   public final OwnableReentrantLock getLock(Object object) {
      return sharedLocks[hashToIndex(object)];
   }

   public final boolean ownsLock(Object object, Object owner) {
      OwnableReentrantLock lock = getLock(object);
      return owner.equals(lock.getOwner());
   }

   public final boolean isLocked(Object object) {
      OwnableReentrantLock lock = getLock(object);
      return lock.isLocked();
   }

   public final int getNumLocksHeld() {
      int i = 0;
      for (OwnableReentrantLock l : sharedLocks) if (l.isLocked()) i++;
      return i;
   }

   public String toString() {
      return "OwnableReentrantStripedLockContainer{" +
            "sharedLocks=" + (sharedLocks == null ? null : Arrays.asList(sharedLocks)) +
            '}';
   }

   public int size() {
      return sharedLocks.length;
   }

   @Override
   protected boolean tryLock(OwnableReentrantLock lock, long timeout, TimeUnit unit, InvocationContext ctx) throws InterruptedException {
      return lock.tryLock(ctx.getLockOwner(), timeout, unit);
   }

   @Override
   protected void unlock(OwnableReentrantLock l, InvocationContext ctx) {
      l.unlock(ctx.getLockOwner());
   }
}
