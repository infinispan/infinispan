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
package org.infinispan.topology;

import org.infinispan.CacheException;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.responses.UnsuccessfulResponse;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * A control command for all cache membership/rebalance operations.
 * It is not a {@code CacheRpcCommand} because it needs to run on the coordinator even when
 * the coordinator doesn't have a certain cache running.
 *
 * @author Dan Berindei
 * @since 5.2
 */
public class CacheTopologyControlCommand implements ReplicableCommand {

   public enum Type {
      // Member to coordinator:
      // A node is requesting to join the cluster.
      JOIN,
      // A member is signaling that it wants to leave the cluster.
      LEAVE,
      // A member is confirming that it finished the rebalance operation.
      REBALANCE_CONFIRM,

      // Coordinator to member:
      // The coordinator is updating the consistent hash.
      // Used to signal the end of rebalancing as well.
      CH_UPDATE,
      // The coordinator is starting a rebalance operation.
      REBALANCE_START,
      // The coordinator is requesting information about the running caches.
      GET_STATUS,

      // Member to coordinator:
      // Enable/disable rebalancing, check whether rebalancing is enabled
      POLICY_DISABLE,
      POLICY_ENABLE,
      POLICY_GET_STATUS,
   }

   private static final Log log = LogFactory.getLog(CacheTopologyControlCommand.class);

   public static final byte COMMAND_ID = 17;

   private transient LocalTopologyManager localTopologyManager;
   private transient ClusterTopologyManager clusterTopologyManager;
   private transient RebalancePolicy rebalancePolicy;

   private String cacheName;
   private Type type;
   private Address sender;
   private CacheJoinInfo joinInfo;

   // TODO Maybe create a separate command for topology/CH-related stuff; otherwise just replace with a CacheTopology
   private int topologyId;
   private ConsistentHash currentCH;
   private ConsistentHash pendingCH;

   private Throwable throwable;
   private int viewId;

   // For CommandIdUniquenessTest only
   public CacheTopologyControlCommand() {
      this.cacheName = null;
   }

   public CacheTopologyControlCommand(String cacheName, Type type, Address sender, int viewId) {
      this.cacheName = cacheName;
      this.type = type;
      this.sender = sender;
      this.viewId = viewId;
   }

   public CacheTopologyControlCommand(String cacheName, Type type, Address sender, CacheJoinInfo joinInfo, int viewId) {
      this.cacheName = cacheName;
      this.type = type;
      this.sender = sender;
      this.joinInfo = joinInfo;
      this.viewId = viewId;
   }

   public CacheTopologyControlCommand(String cacheName, Type type, Address sender, int topologyId,
                                      Throwable throwable, int viewId) {
      this.cacheName = cacheName;
      this.type = type;
      this.sender = sender;
      this.topologyId = topologyId;
      this.throwable = throwable;
      this.viewId = viewId;
   }

   public CacheTopologyControlCommand(String cacheName, Type type, Address sender, CacheTopology cacheTopology, int viewId) {
      this.cacheName = cacheName;
      this.type = type;
      this.sender = sender;
      this.viewId = viewId;
      this.topologyId = cacheTopology.getTopologyId();
      this.currentCH = cacheTopology.getCurrentCH();
      this.pendingCH = cacheTopology.getPendingCH();
   }

   @Inject
   public void init(LocalTopologyManager localTopologyManager, ClusterTopologyManager clusterTopologyManager,
         RebalancePolicy rebalancePolicy) {
      this.localTopologyManager = localTopologyManager;
      this.clusterTopologyManager = clusterTopologyManager;
      this.rebalancePolicy = rebalancePolicy;
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      final boolean trace = log.isTraceEnabled();
      LogFactory.pushNDC(cacheName, trace);
      try {
         Object responseValue = doPerform();
         return SuccessfulResponse.create(responseValue);
      } catch (InterruptedException e) {
         log.tracef("Command execution %s was interrupted because the cache manager is shutting down", this);
         return UnsuccessfulResponse.INSTANCE;
      } catch (Exception t) {
         log.exceptionHandlingCommand(this, t);
         // todo [anistor] CommandAwareRequestDispatcher does not wrap our exceptions so we have to do it instead
         return new ExceptionResponse(t);
      } finally {
         LogFactory.popNDC(trace);
      }
   }

   private Object doPerform() throws Exception {
      switch (type) {
         // member to coordinator
         case JOIN:
            return clusterTopologyManager.handleJoin(cacheName, sender, joinInfo, viewId);
         case LEAVE:
            clusterTopologyManager.handleLeave(cacheName, sender, viewId);
            return null;
         case REBALANCE_CONFIRM:
            clusterTopologyManager.handleRebalanceCompleted(cacheName, sender, topologyId, throwable, viewId);
            return null;

         // coordinator to member
         case CH_UPDATE:
            localTopologyManager.handleConsistentHashUpdate(cacheName, new CacheTopology(topologyId, currentCH, pendingCH), viewId);
            return null;
         case REBALANCE_START:
            localTopologyManager.handleRebalance(cacheName, new CacheTopology(topologyId, currentCH, pendingCH), viewId);
            return null;
         case GET_STATUS:
            return localTopologyManager.handleStatusRequest(viewId);

         // rebalance policy control
         case POLICY_GET_STATUS:
            return rebalancePolicy.isRebalancingEnabled();
         case POLICY_ENABLE:
            rebalancePolicy.setRebalancingEnabled(true);
            return true;
         case POLICY_DISABLE:
            rebalancePolicy.setRebalancingEnabled(false);
            return true;
         default:
            throw new CacheException("Unknown cache topology control command type " + type);
      }
   }

   public String getCacheName() {
      return cacheName;
   }

   public Address getOrigin() {
      return sender;
   }

   public Type getType() {
      return type;
   }

   public int getTopologyId() {
      return topologyId;
   }

   public ConsistentHash getCurrentCH() {
      return currentCH;
   }

   public ConsistentHash getPendingCH() {
      return pendingCH;
   }

   public Throwable getThrowable() {
      return throwable;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public Object[] getParameters() {
      return new Object[]{cacheName, (byte) type.ordinal(), sender, joinInfo, topologyId, currentCH,
            pendingCH, throwable, viewId};
   }

   @Override
   @SuppressWarnings("unchecked")
   public void setParameters(int commandId, Object[] parameters) {
      int i = 0;
      cacheName = (String) parameters[i++];
      type = Type.values()[(Byte) parameters[i++]];
      sender = (Address) parameters[i++];
      joinInfo = (CacheJoinInfo) parameters[i++];
      topologyId = (Integer) parameters[i++];
      currentCH = (ConsistentHash) parameters[i++];
      pendingCH = (ConsistentHash) parameters[i++];
      throwable = (Throwable) parameters[i++];
      viewId = (Integer) parameters[i++];
   }

   @Override
   public String toString() {
      return "CacheTopologyControlCommand{" +
            "cache=" + cacheName +
            ", type=" + type +
            ", sender=" + sender +
            ", joinInfo=" + joinInfo +
            ", topologyId=" + topologyId +
            ", currentCH=" + currentCH +
            ", pendingCH=" + pendingCH +
            ", throwable=" + throwable +
            ", viewId=" + viewId +
            '}';
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }

   @Override
   public boolean canBlock() {
      return true;
   }
}
