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

import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collection;

/**
 * This command is used by a StateProvider to push cache entries to a StateConsumer.
 *
 * @author anistor@redhat.com
 * @since 5.2
 */
public class StateResponseCommand extends BaseRpcCommand {

   private static final Log log = LogFactory.getLog(StateResponseCommand.class);

   public static final byte COMMAND_ID = 20;

   /**
    * The topology id of the sender at send time.
    */
   private int topologyId;

   /**
    * A collections of state chunks to be transferred.
    */
   private Collection<StateChunk> stateChunks;

   /**
    * This is injected on target node via init() method before the command is performed.
    */
   private StateConsumer stateConsumer;

   private StateResponseCommand() {
      super(null);  // for command id uniqueness test
   }

   public StateResponseCommand(String cacheName) {
      super(cacheName);
   }

   public StateResponseCommand(String cacheName, Address origin, int topologyId, Collection<StateChunk> stateChunks) {
      super(cacheName);
      setOrigin(origin);
      this.topologyId = topologyId;
      this.stateChunks = stateChunks;
   }

   public void init(StateConsumer stateConsumer) {
      this.stateConsumer = stateConsumer;
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      final boolean trace = log.isTraceEnabled();
      LogFactory.pushNDC(cacheName, trace);
      try {
         stateConsumer.applyState(getOrigin(), topologyId, stateChunks);
         return null;
      } finally {
         LogFactory.popNDC(trace);
      }
   }

   @Override
   public boolean isReturnValueExpected() {
      return false;
   }

   @Override
   public boolean canBlock() {
      return true;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public Object[] getParameters() {
      return new Object[]{getOrigin(), topologyId, stateChunks};
   }

   @Override
   public void setParameters(int commandId, Object[] parameters) {
      int i = 0;
      setOrigin((Address) parameters[i++]);
      topologyId = (Integer) parameters[i++];
      stateChunks = (Collection<StateChunk>) parameters[i];
   }

   @Override
   public String toString() {
      return "StateResponseCommand{" +
            "cache=" + cacheName +
            ", origin=" + getOrigin() +
            ", topologyId=" + topologyId +
            '}';
   }
}
