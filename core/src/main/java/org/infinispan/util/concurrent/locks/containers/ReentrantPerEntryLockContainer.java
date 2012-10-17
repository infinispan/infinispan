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
import org.infinispan.util.concurrent.locks.VisibleOwnerReentrantLock;
import org.infinispan.util.concurrent.locks.VisibleOwnerRefCountingReentrantLock;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A per-entry lock container for ReentrantLocks
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class ReentrantPerEntryLockContainer extends AbstractPerEntryLockContainer<VisibleOwnerRefCountingReentrantLock> {

   private static final Log log = LogFactory.getLog(ReentrantPerEntryLockContainer.class);

   @Override
   protected Log getLog() {
      return log;
   }

   public ReentrantPerEntryLockContainer(int concurrencyLevel) {
      super(concurrencyLevel);
   }

   @Override
   protected VisibleOwnerRefCountingReentrantLock newLock() {
      return new VisibleOwnerRefCountingReentrantLock();
   }

   @Override
   public boolean ownsLock(Object key, Object ignored) {
      ReentrantLock l = getLockFromMap(key);
      return l != null && l.isHeldByCurrentThread();
   }

   @Override
   public boolean isLocked(Object key) {
      ReentrantLock l = getLockFromMap(key);
      return l != null && l.isLocked();
   }

   private ReentrantLock getLockFromMap(Object key) {
      return locks.get(key);
   }

   @Override
   protected void unlock(VisibleOwnerRefCountingReentrantLock l, Object unused) {
      l.unlock();
   }

   @Override
   protected boolean tryLock(VisibleOwnerRefCountingReentrantLock lock, long timeout, TimeUnit unit, Object unused) throws InterruptedException {
      return lock.tryLock(timeout, unit);
   }

   @Override
   protected void lock(VisibleOwnerRefCountingReentrantLock lock, Object lockOwner) {
      lock.lock();
   }
}
