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

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * {@code StateTransferLock} implementation.
 *
 * @author anistor@redhat.com
 * @author Dan Berindei
 * @since 5.2
 */
public class StateTransferLockImpl implements StateTransferLock {
   private static final Log log = LogFactory.getLog(StateTransferLockImpl.class);
   private static final boolean trace = log.isTraceEnabled();

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
   public void notifyTransactionDataReceived(int topologyId) {
      if (topologyId < transactionDataTopologyId) {
         throw new IllegalStateException("Cannot set a topology id (" + topologyId +
               ") that is lower that the current one (" + transactionDataTopologyId + ")");
      }
      if (trace) {
         log.tracef("Signalling transaction data received for topology %d", topologyId);
      }
      transactionDataTopologyId = topologyId;
      synchronized (transactionDataLock) {
         transactionDataLock.notifyAll();
      }
   }

   @Override
   public void waitForTransactionData(int expectedTopologyId) throws InterruptedException {
      if (transactionDataTopologyId >= expectedTopologyId)
         return;

      if (trace) {
         log.tracef("Waiting for transaction data for topology %d, current topology is %d", expectedTopologyId,
               transactionDataTopologyId);
      }
      synchronized (transactionDataLock) {
         // Do the comparison inside the synchronized lock
         // otherwise the setter might be able to call notifyAll before we wait()
         while (transactionDataTopologyId < expectedTopologyId) {
            transactionDataLock.wait();
         }
      }
      if (trace) {
         log.tracef("Received transaction data for topology %d, expected topology was %d", transactionDataTopologyId,
               expectedTopologyId);
      }
   }

   @Override
   public void notifyTopologyInstalled(int topologyId) {
      if (topologyId < this.topologyId) {
         throw new IllegalStateException("Cannot set a topology id (" + topologyId +
               ") that is lower that the current one (" + this.topologyId + ")");
      }
      if (trace) {
         log.tracef("Signalling topology %d is installed", topologyId);
      }
      this.topologyId = topologyId;
      synchronized (topologyLock) {
         topologyLock.notifyAll();
      }
   }

   @Override
   public void waitForTopology(int expectedTopologyId) throws InterruptedException {
      if (topologyId >= expectedTopologyId)
         return;

      if (trace) {
         log.tracef("Waiting for topology %d to be installed, current topology is %d", expectedTopologyId, topologyId);
      }
      synchronized (topologyLock) {
         // Do the comparison inside the synchronized lock
         // otherwise the setter might be able to call notifyAll before we wait()
         while (topologyId < expectedTopologyId) {
            topologyLock.wait();
         }
      }
      if (trace) {
         log.tracef("Topology %d is now installed, expected topology was %d", topologyId, expectedTopologyId);
      }
   }
}
