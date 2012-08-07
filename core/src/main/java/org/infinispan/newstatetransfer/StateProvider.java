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

import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.remoting.transport.Address;

import java.util.List;
import java.util.Set;

/**
 * Handles outbound state transfers.
 *
 * @author anistor@redhat.com
 * @since 5.2
 */
public interface StateProvider {

   boolean isStateTransferInProgress();

   /**
    * Receive notification of topology changes. Cancels all outbound transfers to destinations that are no longer members.
    *
    * @param topologyId the new topology id
    * @param rCh
    * @param wCh
    */
   void onTopologyUpdate(int topologyId, ConsistentHash rCh, ConsistentHash wCh);

   /**
    * Gets the list of transactions that affect keys from the given segments. This is invoked in response to a
    * StateRequestCommand of type GET_TRANSACTIONS.
    *
    * @param destination the address of the requester
    * @param topologyId
    * @param segments
    * @return list transactions and locks for the given segments
    */
   List<TransactionInfo> getTransactionsForSegments(Address destination, int topologyId, Set<Integer> segments);

   /**
    * Start to send cache entries that belong to the given set of segments. This is invoked in response to a
    * StateRequestCommand of type START_STATE_TRANSFER.
    *
    * @param destination the address of the requester
    * @param topologyId
    * @param segments
    */
   void startOutboundTransfer(Address destination, int topologyId, Set<Integer> segments);

   /**
    * Cancel sending of cache entries that belong to the given set of segments. This is invoked in response to a
    * StateRequestCommand of type CANCEL_STATE_TRANSFER.
    *
    * @param destination the address of the requester
    * @param topologyId
    * @param segments    the segments that we have to cancel transfer for
    */
   void cancelOutboundTransfer(Address destination, int topologyId, Set<Integer> segments);

   /**
    * Cancels all outgoing state transfers.
    */
   void shutdown();
}
