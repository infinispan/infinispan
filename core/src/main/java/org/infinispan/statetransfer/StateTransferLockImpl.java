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

package org.infinispan.statetransfer;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * {@code StateTransferLock} implementation.
 *
 * @author anistor@redhat.com
 * @author Dan Berindei
 * @since 5.2
 */
public class StateTransferLockImpl implements StateTransferLock {
   private static final Log log = LogFactory.getLog(StateTransferLockImpl.class);

   private final ReadWriteLock ownershipLock = new ReentrantReadWriteLock();

   private volatile int topologyId;
   private final Object topologyLock = new Object();

   private volatile int transactionDataTopologyId;
   private final Object transactionDataLock = new Object();

   @Override
   public void acquireExclusiveTopologyLock() {
      ownershipLock.writeLock().lock();
   }

   @Override
   public void releaseExclusiveTopologyLock() {
      ownershipLock.writeLock().unlock();
   }

   @Override
   public void acquireSharedTopologyLock() {
      ownershipLock.readLock().lock();
   }

   @Override
   public void releaseSharedTopologyLock() {
      ownershipLock.readLock().unlock();
   }

   @Override
   public void transactionDataReceived(int topologyId) {
      this.transactionDataTopologyId = topologyId;
      synchronized (transactionDataLock) {
         transactionDataLock.notifyAll();
      }
   }

   @Override
   public void waitForTransactionData(int expectedTopologyId) throws InterruptedException {
      if (transactionDataTopologyId >= expectedTopologyId)
         return;

      log.tracef("Waiting for transaction data for topology %d, current topology is %d", expectedTopologyId,
            transactionDataTopologyId);
      synchronized (transactionDataLock) {
         // Do the comparison inside the synchronized lock
         // otherwise the setter might be able to call notifyAll before we wait()
         while (transactionDataTopologyId < expectedTopologyId) {
            transactionDataLock.wait();
         }
      }
   }

   @Override
   public void topologyInstalled(int topologyId) {
      this.topologyId = topologyId;
      synchronized (topologyLock) {
         topologyLock.notifyAll();
      }
   }

   @Override
   public void waitForTopology(int expectedTopologyId) throws InterruptedException {
      if (topologyId >= expectedTopologyId)
         return;

      log.tracef("Waiting for topology %d to be installed, current topology is %d", expectedTopologyId,
            topologyId);
      synchronized (topologyLock) {
         // Do the comparison inside the synchronized lock
         // otherwise the setter might be able to call notifyAll before we wait()
         while (topologyId < expectedTopologyId) {
            topologyLock.wait();
         }
      }
   }
}
