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
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Set;

/**
 * This command is used by a StateConsumer to request transactions and cache entries from a StateProvider.
 *
 * @author anistor@redhat.com
 * @since 5.2
 */
public class StateRequestCommand extends BaseRpcCommand {

   private static final Log log = LogFactory.getLog(StateRequestCommand.class);

   public enum Type {
      GET_TRANSACTIONS,
      START_STATE_TRANSFER,
      CANCEL_STATE_TRANSFER
   }

   public static final byte COMMAND_ID = 15;

   private Type type;

   private int topologyId;

   private Set<Integer> segments;

   private StateProvider stateProvider;

   private StateRequestCommand() {
      super(null);  // for command id uniqueness test
   }

   public StateRequestCommand(String cacheName) {
      super(cacheName);
   }

   public StateRequestCommand(String cacheName, Type type, Address origin, int topologyId, Set<Integer> segments) {
      super(cacheName);
      this.type = type;
      setOrigin(origin);
      this.topologyId = topologyId;
      this.segments = segments;
   }

   public void init(StateProvider stateProvider) {
      this.stateProvider = stateProvider;
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      final boolean trace = log.isTraceEnabled();
      LogFactory.pushNDC(cacheName, trace);
      try {
         switch (type) {
            case GET_TRANSACTIONS:
               return stateProvider.getTransactionsForSegments(getOrigin(), topologyId, segments);

            case START_STATE_TRANSFER:
               stateProvider.startOutboundTransfer(getOrigin(), topologyId, segments);
               return null;

            case CANCEL_STATE_TRANSFER:
               stateProvider.cancelOutboundTransfer(getOrigin(), topologyId, segments);
               return null;

            default:
               throw new CacheException("Unknown state request command type: " + type);
         }
      } catch (Exception t) {
         log.exceptionHandlingCommand(this, t);
         return new ExceptionResponse(t);
      } finally {
         LogFactory.popNDC(trace);
      }
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
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
      return new Object[]{(byte) type.ordinal(), getOrigin(), topologyId, segments};
   }

   @Override
   @SuppressWarnings("unchecked")
   public void setParameters(int commandId, Object[] parameters) {
      int i = 0;
      type = Type.values()[(Byte) parameters[i++]];
      setOrigin((Address) parameters[i++]);
      topologyId = (Integer) parameters[i++];
      segments = (Set<Integer>) parameters[i];
   }

   @Override
   public String toString() {
      return "StateRequestCommand{" +
            "cache=" + cacheName +
            ", type=" + type +
            ", origin=" + getOrigin() +
            ", topologyId=" + topologyId +
            '}';
   }
}
