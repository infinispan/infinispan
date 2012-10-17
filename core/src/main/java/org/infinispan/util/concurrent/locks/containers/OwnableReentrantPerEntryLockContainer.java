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

import org.infinispan.util.concurrent.locks.OwnableReentrantLock;
import org.infinispan.util.concurrent.locks.OwnableRefCountingReentrantLock;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.concurrent.TimeUnit;

/**
 * A per-entry lock container for OwnableReentrantLocks
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class OwnableReentrantPerEntryLockContainer extends AbstractPerEntryLockContainer<OwnableRefCountingReentrantLock> {

   private static final Log log = LogFactory.getLog(OwnableReentrantPerEntryLockContainer.class);

   @Override
   protected Log getLog() {
      return log;
   }

   public OwnableReentrantPerEntryLockContainer(int concurrencyLevel) {
      super(concurrencyLevel);
   }

   @Override
   protected OwnableRefCountingReentrantLock newLock() {
      return new OwnableRefCountingReentrantLock();
   }

   @Override
   public boolean ownsLock(Object key, Object owner) {
      OwnableReentrantLock l = getLockFromMap(key);
      return l != null && owner.equals(l.getOwner());
   }

   @Override
   public boolean isLocked(Object key) {
      OwnableReentrantLock l = getLockFromMap(key);
      return l != null && l.isLocked();
   }

   private OwnableReentrantLock getLockFromMap(Object key) {
      return locks.get(key);
   }

   @Override
   protected boolean tryLock(OwnableRefCountingReentrantLock lock, long timeout, TimeUnit unit, Object lockOwner) throws InterruptedException {
      return lock.tryLock(lockOwner, timeout, unit);
   }

   @Override
   protected void lock(OwnableRefCountingReentrantLock lock, Object lockOwner) {
      lock.lock(lockOwner);
   }

   @Override
   protected void unlock(OwnableRefCountingReentrantLock l, Object owner) {
      l.unlock(owner);
   }
}
