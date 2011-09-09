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
package org.infinispan.commands.remote;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.tx.TransactionBoundaryCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Arrays;
import java.util.List;

/**
 * Command that implements cluster replication logic.
 * <p/>
 * This is not a {@link VisitableCommand} and hence not passed up the {@link org.infinispan.interceptors.base.CommandInterceptor}
 * chain.
 * <p/>
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class MultipleRpcCommand extends BaseRpcInvokingCommand {

   public static final byte COMMAND_ID = 2;

   private static final Log log = LogFactory.getLog(MultipleRpcCommand.class);
   private static final boolean trace = log.isTraceEnabled();

   private ReplicableCommand[] commands;

   private MultipleRpcCommand() {
      super(null); // For command id uniqueness test
   }

   public MultipleRpcCommand(List<ReplicableCommand> modifications, String cacheName) {
      super(cacheName);
      commands = modifications.toArray(new ReplicableCommand[modifications.size()]);
   }

   public MultipleRpcCommand(String cacheName) {
      super(cacheName);
   }

   /**
    * Executes commands replicated to the current cache instance by other cache instances.
    */
   public Object perform(InvocationContext ctx) throws Throwable {
      if (trace) log.tracef("Executing remotely originated commands: %d", commands.length);
      for (ReplicableCommand command : commands) {
         if (command instanceof TransactionBoundaryCommand) {
            command.perform(null);
         } else {
            processVisitableCommand(command);
         }
      }
      return null;
   }

   public byte getCommandId() {
      return COMMAND_ID;
   }

   public ReplicableCommand[] getCommands() {
      return commands;
   }

   public Object[] getParameters() {
      int numCommands = commands.length;
      Object[] retval = new Object[numCommands];
      System.arraycopy(commands, 0, retval, 0, numCommands);
      return retval;
   }

   @SuppressWarnings("unchecked")
   public void setParameters(int commandId, Object[] args) {
      int numCommands = args.length;
      commands = new ReplicableCommand[numCommands];
      System.arraycopy(args, 0, commands, 0, numCommands);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof MultipleRpcCommand)) return false;

      MultipleRpcCommand that = (MultipleRpcCommand) o;

      if (cacheName != null ? !cacheName.equals(that.cacheName) : that.cacheName != null) return false;
      if (!Arrays.equals(commands, that.commands)) return false;
      if (interceptorChain != null ? !interceptorChain.equals(that.interceptorChain) : that.interceptorChain != null)
         return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = interceptorChain != null ? interceptorChain.hashCode() : 0;
      result = 31 * result + (commands != null ? Arrays.hashCode(commands) : 0);
      result = 31 * result + (cacheName != null ? cacheName.hashCode() : 0);
      return result;
   }

   @Override
   public String toString() {
      return "MultipleRpcCommand{" +
            "commands=" + (commands == null ? null : Arrays.asList(commands)) +
            ", cacheName='" + cacheName + '\'' +
            '}';
   }
}