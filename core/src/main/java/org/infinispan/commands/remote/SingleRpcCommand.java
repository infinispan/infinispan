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
import org.infinispan.context.InvocationContext;

/**
 * Similar to {@link org.infinispan.commands.remote.MultipleRpcCommand}, but it only aggregates a single command for
 * replication.
 *
 * @author Mircea.Markus@jboss.com
 */
public class SingleRpcCommand extends BaseRpcInvokingCommand {
   public static final int COMMAND_ID = 1;

   private ReplicableCommand command;

   private SingleRpcCommand() {
      super(null); // For command id uniqueness test
   }

   public SingleRpcCommand(String cacheName, ReplicableCommand command) {
      super(cacheName);
      this.command = command;
   }

   public SingleRpcCommand(String cacheName) {
      super(cacheName);
   }

   public void setParameters(int commandId, Object[] parameters) {
      if (commandId != COMMAND_ID) throw new IllegalStateException("Unusupported command id:" + commandId);
      command = (ReplicableCommand) parameters[0];
   }

   public byte getCommandId() {
      return COMMAND_ID;
   }

   public Object[] getParameters() {
      return new Object[]{command};
   }

   public Object perform(InvocationContext ctx) throws Throwable {
      return processVisitableCommand(command);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof SingleRpcCommand)) return false;

      SingleRpcCommand that = (SingleRpcCommand) o;

      if (cacheName != null ? !cacheName.equals(that.cacheName) : that.cacheName != null) return false;
      if (command != null ? !command.equals(that.command) : that.command != null) return false;
      if (interceptorChain != null ? !interceptorChain.equals(that.interceptorChain) : that.interceptorChain != null)
         return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = interceptorChain != null ? interceptorChain.hashCode() : 0;
      result = 31 * result + (cacheName != null ? cacheName.hashCode() : 0);
      result = 31 * result + (command != null ? command.hashCode() : 0);
      return result;
   }

   @Override
   public String toString() {
      return "SingleRpcCommand{" +
            "cacheName='" + cacheName + '\'' +
            ", command=" + command +
            '}';
   }

   public ReplicableCommand getCommand() {
      return command;
   }
}
