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

import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.distribution.ch.AdvancedConsistentHash;
import org.infinispan.remoting.transport.Address;

import java.util.Collection;

/**
 * Handles inbound state transfers.
 *
 * @author anistor@redhat.com
 * @since 5.2
 */
public interface StateConsumer {

   boolean isStateTransferInProgress();

   boolean isStateTransferInProgressForKey(Object key);

   /**
    * Receive notification of topology changes. StateRequestCommands are issued for segments that are new to this member
    * and the segments that are no longer owned are discarded.
    *
    * @param topologyId the new topology id
    * @param newCh      the new consistent hash
    */
   void onTopologyUpdate(int topologyId, AdvancedConsistentHash newCh);

   void applyTransactions(Address sender, int topologyId, Collection<TransactionInfo> transactions);

   void applyState(Address sender, int topologyId, int segmentId, Collection<InternalCacheEntry> cacheEntries);

   /**
    * Cancels all incoming state transfers. The already received data is not discarded.
    */
   void shutdown();
}
