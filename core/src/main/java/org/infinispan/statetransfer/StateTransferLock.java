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

import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

/**
 * We use the state transfer lock for three different things:
 * <ol>
 *    <li>We don't want to execute a command until we have the transaction table for that topology id.
 *    For this purpose it works like a latch, commands wait on the latch and state transfer opens the latch
 *    when it has received all the transaction data for that topology id.</li>
 *    <li>Do not write anything to the data container in a segment that we have already removed.
 *    For this purpose, ownership checks and data container writes acquire a shared lock, and
 *    the segment removal acquires an exclusive lock.</li>
 *    <li>We want to handle state requests only after we have installed the same topology id, because
 *    this guarantees that we also have installed the corresponding view id and we have all the joiners
 *    in our JGroups view. Here it works like a latch as well, state requests wait on the latch and state
 *    transfer opens the latch when it has received all the transaction data for that topology id.</li>
 * </ol>
 *
 * @author anistor@redhat.com
 * @author Dan Berindei
 * @since 5.2
 */
@Scope(Scopes.NAMED_CACHE)
public interface StateTransferLock {

   // topology change lock
   void acquireExclusiveTopologyLock();

   void releaseExclusiveTopologyLock();

   void acquireSharedTopologyLock();

   void releaseSharedTopologyLock();

   // transaction data latch
   void notifyTransactionDataReceived(int topologyId);

   void waitForTransactionData(int expectedTopologyId) throws InterruptedException;

   // topology installation latch
   // TODO move this to Cluster/LocalTopologyManagerImpl and don't start requesting state until every node has the jgroups view with the local node
   void notifyTopologyInstalled(int topologyId);

   void waitForTopology(int expectedTopologyId) throws InterruptedException;
}