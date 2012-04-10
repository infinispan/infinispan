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
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.statetransfer.LockInfo;
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collection;

/**
 * A control command to coordinate state transfer that may occur when nodes join or leave a cluster.
 * It is used both in replicated mode and in distributed mode.
 *
 * @author Manik Surtani
 * @author Vladimir Blagojevic
 * @author Dan Berindei <dan@infinispan.org>
 * @author Mircea Markus
 * @since 4.0
 */
public class StateTransferControlCommand extends BaseRpcCommand {

   public static final int COMMAND_ID = 15;

   /* For a detailed description of the interactions involved here, please visit http://community.jboss.org/wiki/DesignOfDynamicRehashing */
   public enum Type {
      // receive a map of keys and add them to the data container
      APPLY_STATE,
      APPLY_LOCKS
   }

   Type type;
   Address sender;
   int viewId;
   Collection<InternalCacheEntry> state;
   Collection<LockInfo> locks;


   // cache components
   StateTransferManager stateTransferManager;
   Configuration configuration;
   DataContainer dataContainer;
   CommandsFactory commandsFactory;
   private static final Log log = LogFactory.getLog(StateTransferControlCommand.class);

   public StateTransferControlCommand() {
      super(null);
   }

   public StateTransferControlCommand(String cacheName) {
      super(cacheName);
   }

   public StateTransferControlCommand(String cacheName, Type type, Address sender, int viewId, Collection<InternalCacheEntry> state, Collection<LockInfo> lockInfo) {
      super(cacheName);
      this.type = type;
      this.sender = sender;
      this.viewId = viewId;
      this.state = state;
      this.locks = lockInfo;
   }

   public StateTransferControlCommand(String cacheName, Type type, Address sender, int viewId) {
      super(cacheName);
      this.type = type;
      this.sender = sender;
      this.viewId = viewId;
   }

   public void init(StateTransferManager stateTransferManager, Configuration configuration, DataContainer dataContainer,
                    CommandsFactory commandsFactory) {
      this.stateTransferManager = stateTransferManager;
      this.configuration = configuration;
      this.dataContainer = dataContainer;
      this.commandsFactory = commandsFactory;
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      final boolean trace = log.isTraceEnabled();
      LogFactory.pushNDC(configuration.getName(), trace);
      stateTransferManager.waitForJoinToStart();
      try {
         switch (type) {
            case APPLY_STATE:
               stateTransferManager.applyState(state, sender, viewId);
               return null;
            case APPLY_LOCKS:
               stateTransferManager.applyLocks(locks, sender, viewId);
               return null;
            default:
               throw new CacheException("Unknown rehash control command type " + type);
         }
      } catch (Throwable t) {
         log.exceptionHandlingCommand(this, t);
         return null;
      } finally {
         LogFactory.popNDC(trace);
      }
   }

   public Type getType() {
      return type;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public Object[] getParameters() {
      return new Object[]{(byte) type.ordinal(), sender, viewId, state, locks};
   }

   @Override
   @SuppressWarnings("unchecked")
   public void setParameters(int commandId, Object[] parameters) {
      int i = 0;
      type = Type.values()[(Byte) parameters[i++]];
      sender = (Address) parameters[i++];
      viewId = (Integer) parameters[i++];
      state = (Collection<InternalCacheEntry>) parameters[i++];
      locks = (Collection<LockInfo>) parameters[i];
   }

   @Override
   public String toString() {
      return "StateTransferControlCommand{" +
            "cache=" + cacheName +
            ", type=" + type +
            ", sender=" + sender +
            ", viewId=" + viewId +
            ", state=" + (state == null ? "N/A" : state.size()) +
            ", locks=" + (locks == null ? "N/A" : locks.size()) +
            '}';
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }
}
