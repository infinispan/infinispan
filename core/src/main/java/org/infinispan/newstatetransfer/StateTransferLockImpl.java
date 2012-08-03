/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other
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

package org.infinispan.newstatetransfer;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * // TODO: Document this
 *
 * @author anistor@redhat.com
 * @since 5.2
 */
public class StateTransferLockImpl implements StateTransferLock {

   private final ReadWriteLock transactionTableLock = new ReentrantReadWriteLock();

   private volatile boolean isStateTransferInProgress = false;

   @Override
   public void acquireTTSharedLock() {
      transactionTableLock.readLock().lock();
   }

   @Override
   public void releaseTTSharedLock() {
      transactionTableLock.readLock().unlock();
   }

   @Override
   public void acquireTTExclusiveLock() {
      transactionTableLock.writeLock().lock();
   }

   @Override
   public void releaseTTExclusiveLock() {
      transactionTableLock.writeLock().unlock();
   }

   @Override
   public boolean isStateTransferInProgress() {
      return isStateTransferInProgress;
   }

   @Override
   public void setStateTransferInProgress(boolean isStateTransferInProgress) {
      this.isStateTransferInProgress = isStateTransferInProgress;
   }
}
