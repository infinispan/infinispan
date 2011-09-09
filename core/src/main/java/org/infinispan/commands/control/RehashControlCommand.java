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
package org.infinispan.commands.control;

import org.infinispan.CacheException;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.config.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Map;

/**
 * A control command to coordinate rehashes that may occur when nodes join or leave a cluster, when DIST is used as a
 * cache mode.  This complex command coordinates the various phases of a rehash event when a joiner joins or a leaver
 * leaves a cluster running in "distribution" mode.
 * <p />
 * It may break up into several commands in future.
 * <p />
 * In its current form, it is documented on <a href="http://community.jboss.org/wiki/DesignOfDynamicRehashing">this wiki page</a>.
 *
 * @author Manik Surtani
 * @author Vladimir Blagojevic
 * @since 4.0
 */
public class RehashControlCommand extends BaseRpcCommand {

   public static final int COMMAND_ID = 17;

   /* For a detailed description of the interactions involved here, please visit http://community.jboss.org/wiki/DesignOfDynamicRehashing */
   public enum Type {
      // receive a map of keys and add them to the data container
      APPLY_STATE,
      // a node signals to the coordinator that it finished pushing state
      NODE_PUSH_COMPLETED,
      // the coordinator signals that every node in the cluster has finished pushing state
      REHASH_COMPLETED
   }

   Type type;
   Address sender;
   int viewId;
   Map<Object, InternalCacheValue> state;
   ConsistentHash oldCH;
   ConsistentHash newCH;

   // cache components
   DistributionManager distributionManager;
   Transport transport;
   Configuration configuration;
   DataContainer dataContainer;
   CommandsFactory commandsFactory;
   private static final Log log = LogFactory.getLog(RehashControlCommand.class);

   private RehashControlCommand() {
      super(null); // For command id uniqueness test
   }

   public RehashControlCommand(String cacheName, Type type, Address sender, int viewId, Map<Object, InternalCacheValue> state, ConsistentHash oldConsistentHash,
                                ConsistentHash consistentHash) {
      super(cacheName);
      this.type = type;
      this.sender = sender;
      this.viewId = viewId;
      this.state = state;
      this.oldCH = oldConsistentHash;
      this.newCH = consistentHash;
   }

   public RehashControlCommand(String cacheName, Type type, Address sender, int viewId) {
      super(cacheName);
      this.type = type;
      this.sender = sender;
      this.viewId = viewId;
   }

   public RehashControlCommand(String cacheName, Transport transport) {
      super(cacheName);
      this.transport = transport;
   }

   public void init(DistributionManager distributionManager, Configuration configuration, DataContainer dataContainer,
                    CommandsFactory commandsFactory) {
      if (!configuration.getCacheMode().isDistributed()) {
         log.rehashCommandReceivedOnNonDistributedCache();
         throw new IllegalStateException("Rehash command received on non-distributed cache");
      }
      this.distributionManager = distributionManager;
      this.configuration = configuration;
      this.dataContainer = dataContainer;
      this.commandsFactory = commandsFactory;
   }

   public Object perform(InvocationContext ctx) throws Throwable {
      LogFactory.pushNDC(configuration.getName(), log.isTraceEnabled());
      try {
         switch (type) {
            case APPLY_STATE:
               distributionManager.applyState(newCH, state, sender, viewId);
               return null;
            case NODE_PUSH_COMPLETED:
               distributionManager.markNodePushCompleted(viewId, sender);
               return null;
            case REHASH_COMPLETED:
               distributionManager.markRehashCompleted(viewId);
               return null;
         }
      } finally {
         LogFactory.popNDC(log.isTraceEnabled());
      }
      throw new CacheException("Unknown rehash control command type " + type);
   }

   public Type getType() {
      return type;
   }

   public byte getCommandId() {
      return COMMAND_ID;
   }

   public Object[] getParameters() {
      return new Object[]{(byte) type.ordinal(), sender, viewId, state, oldCH, newCH};
   }

   @SuppressWarnings("unchecked")
   public void setParameters(int commandId, Object[] parameters) {
      int i = 0;
      type = Type.values()[(Byte) parameters[i++]];
      sender = (Address) parameters[i++];
      viewId = (Integer) parameters[i++];
      state = (Map<Object, InternalCacheValue>) parameters[i++];
      oldCH = (ConsistentHash) parameters[i++];
      newCH = (ConsistentHash) parameters[i++];
   }

   @Override
   public String toString() {
      return "RehashControlCommand{" +
            "cache=" + cacheName +
            ", type=" + type +
            ", sender=" + sender +
            ", viewId=" + viewId +
            ", state=" + (state == null ? "N/A" : state.size()) +
            ", oldConsistentHash=" + oldCH +
            ", consistentHash=" + newCH +
            '}';
   }
}
