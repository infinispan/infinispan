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

import org.infinispan.commands.CommandsFactory;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;

import java.util.*;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * // TODO [anistor] Document this
 *
 * @author anistor@redhat.com
 * @since 5.2
 */
public class InboundTransferTask {

   private final Set<Integer> segments = new CopyOnWriteArraySet<Integer>();

   private final Set<Integer> finishedSegments = new CopyOnWriteArraySet<Integer>();

   private final Address source;

   private volatile boolean isCancelled = false;

   private final StateConsumerImpl stateConsumer;

   private final int topologyId;

   private final RpcManager rpcManager;

   private final CommandsFactory commandsFactory;

   private final long timeout;

   public InboundTransferTask(Set<Integer> segments, Address source, int topologyId, StateConsumerImpl stateConsumer, RpcManager rpcManager, CommandsFactory commandsFactory, long timeout) {
      if (segments == null || segments.isEmpty()) {
         throw new IllegalArgumentException("segments must not be null or empty");
      }
      if (source == null) {
         throw new IllegalArgumentException("Source address cannot be null");
      }

      this.segments.addAll(segments);
      this.source = source;
      this.topologyId = topologyId;
      this.stateConsumer = stateConsumer;
      this.rpcManager = rpcManager;
      this.commandsFactory = commandsFactory;
      this.timeout = timeout;
   }

   public Set<Integer> getSegments() {
      return segments;
   }

   public Address getSource() {
      return source;
   }

   public void getTransactions() {
      // get transactions and locks
      StateRequestCommand cmd = commandsFactory.buildStateRequestCommand(StateRequestCommand.Type.GET_TRANSACTIONS, rpcManager.getAddress(), topologyId, segments);
      Map<Address, Response> responses = rpcManager.invokeRemotely(Collections.singleton(source), cmd, ResponseMode.SYNCHRONOUS_IGNORE_LEAVERS, timeout);
      Response response = responses.get(source);
      if (response instanceof SuccessfulResponse) {
         List<TransactionInfo> transactions = (List<TransactionInfo>) ((SuccessfulResponse) response).getResponseValue();
         stateConsumer.applyTransactions(source, topologyId, transactions);
      } else {
         //todo [anistor] fail
      }
      //todo [anistor] unlock transactions after we have received transactions from all donors
   }

   public void requestSegments() {
      // start transfer of cache entries
      StateRequestCommand cmd = commandsFactory.buildStateRequestCommand(StateRequestCommand.Type.START_STATE_TRANSFER, rpcManager.getAddress(), topologyId, segments);
      rpcManager.invokeRemotely(Collections.singleton(source), cmd, ResponseMode.SYNCHRONOUS_IGNORE_LEAVERS, timeout);
   }

   public void cancelSegments(Set<Integer> cancelledSegments) {
      if (cancelledSegments.retainAll(segments)) {
         throw new IllegalArgumentException("Some of the specified segments cannot be cancelled because they were not previously requested");
      }

      segments.removeAll(cancelledSegments);
      finishedSegments.removeAll(cancelledSegments);
      if (segments.isEmpty()) {
         isCancelled = true;
      }

      StateRequestCommand cmd = commandsFactory.buildStateRequestCommand(StateRequestCommand.Type.CANCEL_STATE_TRANSFER, rpcManager.getAddress(), topologyId, cancelledSegments);
      rpcManager.invokeRemotely(Collections.singleton(source), cmd, ResponseMode.SYNCHRONOUS_IGNORE_LEAVERS, timeout);
   }

   public void cancel() {
      if (!isCancelled) {
         isCancelled = true;

         Set<Integer> cancelledSegments = new HashSet<Integer>(segments);
         segments.clear();
         finishedSegments.clear();

         StateRequestCommand cmd = commandsFactory.buildStateRequestCommand(StateRequestCommand.Type.CANCEL_STATE_TRANSFER, rpcManager.getAddress(), topologyId, cancelledSegments);
         rpcManager.invokeRemotely(Collections.singleton(source), cmd, ResponseMode.SYNCHRONOUS_IGNORE_LEAVERS, timeout);

         stateConsumer.onTaskCompletion(this);
      }
   }

   public void onStateReceived(int segmentId, int numEntries) {
      if (!isCancelled && segments.contains(segmentId)) {
         if (numEntries == 0) {
            finishedSegments.add(segmentId);
            if (finishedSegments.containsAll(segments)) {
               stateConsumer.onTaskCompletion(this);
            }
         }
      }
   }
}
