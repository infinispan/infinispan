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

import org.infinispan.CacheException;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Inbound state transfer task. Fetches multiple data segments from a remote source node and applies them to local
 * cache. Instances of InboundTransferTask are created and managed by StateTransferManagerImpl. StateTransferManagerImpl
 * must have zero or one such task for each segment.
 *
 * @author anistor@redhat.com
 * @since 5.2
 */
public class InboundTransferTask {

   private static final Log log = LogFactory.getLog(InboundTransferTask.class);
   private static final boolean trace = log.isTraceEnabled();

   private final Set<Integer> segments = new CopyOnWriteArraySet<Integer>();

   private final Set<Integer> finishedSegments = new CopyOnWriteArraySet<Integer>();

   private final Address source;

   private volatile boolean isCancelled = false;

   /**
    * Indicates if the request was sent to source.
    */
   private final AtomicBoolean isStarted = new AtomicBoolean();

   /**
    * Indicates if the START_STATE_TRANSFER was successfully sent to source node and the source replied with a successful response.
    */
   private boolean isStartedSuccessfully = false;

   /**
    * Indicates if the task was completed normally: all requested segments were completely received except for the cancelled ones.
    * This flag is only meaningful when completionLatch becomes 0.
    */
   private volatile boolean isCompletedSuccessfully = false;

   /**
    * This latch is counted down when all segments are completely received or in case of task cancellation.
    */
   private final CountDownLatch completionLatch = new CountDownLatch(1);

   private final StateConsumerImpl stateConsumer;

   private final int topologyId;

   private final RpcManager rpcManager;

   private final CommandsFactory commandsFactory;

   private final long timeout;

   private final String cacheName;

   private final RpcOptions rpcOptions;

   public InboundTransferTask(Set<Integer> segments, Address source, int topologyId, StateConsumerImpl stateConsumer, RpcManager rpcManager, CommandsFactory commandsFactory, long timeout, String cacheName) {
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
      this.cacheName = cacheName;
      //the rpc options does not changed in runtime and they are the same in all the remote invocations. re-use the
      //same instance
      this.rpcOptions = rpcManager.getRpcOptionsBuilder(ResponseMode.SYNCHRONOUS_IGNORE_LEAVERS)
            .timeout(timeout, TimeUnit.MILLISECONDS).build();
   }

   public Set<Integer> getSegments() {
      return segments;
   }

   public Set<Integer> getUnfinishedSegments() {
      Set<Integer> unfinishedSegments = new HashSet<Integer>(segments);
      unfinishedSegments.removeAll(finishedSegments);
      return unfinishedSegments;
   }

   public Address getSource() {
      return source;
   }

   /**
    * Send START_STATE_TRANSFER request to source node.
    */
   public boolean requestSegments() {
      if (!isCancelled && isStarted.compareAndSet(false, true)) {
         if (trace) {
            log.tracef("Requesting segments %s of cache %s from node %s", segments, cacheName, source);
         }
         // start transfer of cache entries
         try {
            StateRequestCommand cmd = commandsFactory.buildStateRequestCommand(StateRequestCommand.Type.START_STATE_TRANSFER, rpcManager.getAddress(), topologyId, segments);
            Map<Address, Response> responses = rpcManager.invokeRemotely(Collections.singleton(source), cmd, rpcOptions);
            Response response = responses.get(source);
            if (response instanceof SuccessfulResponse) {
               isStartedSuccessfully = true;
               if (trace) {
                  log.tracef("Successfully requested segments %s of cache %s from node %s", segments, cacheName, source);
               }
               return true;
            }
            log.failedToRequestSegments(segments, cacheName, source, null);
         } catch (CacheException e) {
            log.failedToRequestSegments(segments, cacheName, source, e);
         }
         return false;
      } else {
         return true;
      }
   }

   /**
    * Cancels a subset of the segments. If it happens that all segments are cancelled then the whole task is marked as cancelled and completion is signalled..
    *
    * @param cancelledSegments the segments to be cancelled
    */
   public void cancelSegments(Set<Integer> cancelledSegments) {
      if (isCancelled) {
         throw new IllegalArgumentException("The task is already cancelled.");
      }
      if (cancelledSegments.retainAll(segments)) {
         throw new IllegalArgumentException("Some of the specified segments cannot be cancelled because they were not previously requested");
      }

      if (trace) {
         log.tracef("Cancelling inbound state transfer of segments %s of cache %s", cancelledSegments, cacheName);
      }

      segments.removeAll(cancelledSegments);
      finishedSegments.removeAll(cancelledSegments);
      if (segments.isEmpty()) {
         isCancelled = true;
      }

      sendCancelCommand(cancelledSegments);

      if (isCancelled) {
         notifyCompletion();
      }
   }

   public void cancel() {
      if (!isCancelled) {
         isCancelled = true;

         if (trace) {
            log.tracef("Cancelling inbound state transfer of segments %s of cache %s", segments, cacheName);
         }

         sendCancelCommand(segments);

         notifyCompletion();
      }
   }

   private void sendCancelCommand(Set<Integer> cancelledSegments) {
      StateRequestCommand cmd = commandsFactory.buildStateRequestCommand(
            StateRequestCommand.Type.CANCEL_STATE_TRANSFER, rpcManager.getAddress(), topologyId,
            cancelledSegments);
      rpcManager.invokeRemotely(Collections.singleton(source), cmd, rpcManager.getDefaultRpcOptions(false));
   }

   public void onStateReceived(int segmentId, boolean isLastChunk) {
      if (!isCancelled && isLastChunk && segments.contains(segmentId)) {
         finishedSegments.add(segmentId);
         if (finishedSegments.containsAll(segments)) {
            if (trace) {
               log.tracef("Finished receiving state for segments %s of cache %s", segments, cacheName);
            }
            notifyCompletion();
         }
      }
   }

   private void notifyCompletion() {
      isCompletedSuccessfully = true;
      stateConsumer.onTaskCompletion(this);
      completionLatch.countDown();
   }

   /**
    * Wait until all segments are received, cancelled, or the task is terminated abruptly by <code>terminate()</code>.
    *
    * @return true if the task completed normally or false if it was terminated abruptly
    * @throws InterruptedException if the thread is interrupted while waiting
    */
   public boolean awaitCompletion() throws InterruptedException {
      if (!isStartedSuccessfully) {
         throw new IllegalStateException("Cannot await completion unless the request was previously sent to source node successfully.");
      }
      completionLatch.await();
      return isCompletedSuccessfully;
   }

   /**
    * Terminate abruptly regardless if the segments were received or not. This is used when the source node
    * is no longer alive.
    */
   public void terminate() {
      completionLatch.countDown();
   }

   @Override
   public String toString() {
      return "InboundTransferTask{" +
            "segments=" + segments +
            ", finishedSegments=" + finishedSegments +
            ", unfinishedSegments=" + getUnfinishedSegments() +
            ", source=" + source +
            ", isCancelled=" + isCancelled +
            ", isStartedSuccessfully=" + isStartedSuccessfully +
            ", isCompletedSuccessfully=" + isCompletedSuccessfully +
            ", topologyId=" + topologyId +
            ", timeout=" + timeout +
            ", cacheName=" + cacheName +
            '}';
   }
}
