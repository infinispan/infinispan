/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2000 - 2008, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
package org.horizon.commands.remote;

import org.horizon.commands.CacheRPCCommand;
import org.horizon.commands.ReplicableCommand;
import org.horizon.commands.VisitableCommand;
import org.horizon.context.InvocationContext;
import org.horizon.interceptors.InterceptorChain;
import org.horizon.logging.Log;
import org.horizon.logging.LogFactory;

import java.util.Arrays;
import java.util.List;

/**
 * Command that implements cluster replication logic.
 * <p/>
 * This is not a {@link VisitableCommand} and hence not passed up the {@link org.horizon.interceptors.base.CommandInterceptor}
 * chain.
 * <p/>
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class ReplicateCommand implements CacheRPCCommand {
   public static final byte METHOD_ID = 13;

   private InterceptorChain interceptorChain;

   private static final Log log = LogFactory.getLog(ReplicateCommand.class);
   private static final boolean trace = log.isTraceEnabled();

   private ReplicableCommand[] commands;
   private String cacheName;

   public ReplicateCommand(List<ReplicableCommand> modifications, String cacheName) {
      if (modifications != null && modifications.size() == 1) {
         this.commands = new ReplicableCommand[]{modifications.get(0)};
      } else {
         this.commands = new ReplicableCommand[modifications.size()];
         int i = 0;
         for (ReplicableCommand rc : modifications) commands[i++] = rc;
      }
      this.cacheName = cacheName;
   }

   public ReplicateCommand(ReplicableCommand command, String cacheName) {
      commands = new ReplicableCommand[]{command};
      this.cacheName = cacheName;
   }

   public ReplicateCommand() {
   }

   public void setInterceptorChain(InterceptorChain interceptorChain) {
      this.interceptorChain = interceptorChain;
   }

   /**
    * Executes commands replicated to the current cache instance by other cache instances.
    *
    * @param ctx invocation context, ignored.
    * @return null
    * @throws Throwable
    */
   public Object perform(InvocationContext ctx) throws Throwable {
      if (isSingleCommand()) {
         return processCommand(ctx, commands[0]);
      } else {
         for (ReplicableCommand command : commands) processCommand(ctx, command);
         return null;
      }
   }

   private Object processCommand(InvocationContext ctx, ReplicableCommand cacheCommand) throws Throwable {
      Object result;
      try {
         if (trace) log.trace("Invoking command " + cacheCommand + ", with originLocal flag set to false.");
         ctx.setOriginLocal(false);
         if (cacheCommand instanceof VisitableCommand) {
            Object retVal = interceptorChain.invokeRemote((VisitableCommand) cacheCommand);
            // we only need to return values for a set of remote calls; not every call.
            if (returnValueForRemoteCall(cacheCommand)) {
               result = retVal;
            } else {
               result = null;
            }
         } else {
            throw new RuntimeException("Do we still need to deal with non-visitable commands? (" + cacheCommand.getClass().getName() + ")");
//            result = cacheCommand.perform(null);
         }
      }
      catch (Throwable ex) {
         // TODO deal with PFER
//         if (!(cacheCommand instanceof PutForExternalReadCommand))
//         {
         throw ex;
//         }
//         else
//         {
//            if (trace)
//               log.trace("Caught an exception, but since this is a putForExternalRead() call, suppressing the exception.  Exception is:", ex);
//            result = null;
//         }
      }
      return result;
   }

   private boolean returnValueForRemoteCall(ReplicableCommand cacheCommand) {
      return cacheCommand instanceof ClusteredGetCommand;
   }

   public byte getCommandId() {
      return METHOD_ID;
   }

   public ReplicableCommand[] getCommands() {
      return commands;
   }

   public String getCacheName() {
      return cacheName;
   }

   public void setCacheName(String name) {
      this.cacheName = name;
   }

   public final ReplicableCommand getSingleCommand() {
      return commands == null ? null : commands[0];
   }

   public Object[] getParameters() {
      int numCommands = commands == null ? 0 : commands.length;
      Object[] retval = new Object[numCommands + 2];
      retval[0] = cacheName;
      retval[1] = numCommands;
      if (numCommands > 0) System.arraycopy(commands, 0, retval, 2, numCommands);
      return retval;
   }

   @SuppressWarnings("unchecked")
   public void setParameters(int commandId, Object[] args) {
      cacheName = (String) args[0];
      int numCommands = (Integer) args[1];
      commands = new ReplicableCommand[numCommands];
      System.arraycopy(args, 2, commands, 0, numCommands);
   }

   public final boolean isSingleCommand() {
      return commands != null && commands.length == 1;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      ReplicateCommand that = (ReplicateCommand) o;

      return !(commands != null ? !commands.equals(that.commands) : that.commands != null);
   }

   @Override
   public int hashCode() {
      return commands != null ? commands.hashCode() : 0;
   }

   /**
    * Creates a copy of this command, amking a deep copy of any collections but everything else copied shallow.
    *
    * @return a copy
    */
   public ReplicateCommand copy() {
      ReplicateCommand clone;
      clone = new ReplicateCommand();
      clone.interceptorChain = interceptorChain;
      if (commands != null) clone.commands = commands.clone();
      return clone;
   }

   public boolean containsCommandType(Class<? extends ReplicableCommand> aClass) {
      if (commands.length == 1) {
         return commands[0].getClass().equals(aClass);
      } else {
         for (ReplicableCommand command : getCommands()) {
            if (command.getClass().equals(aClass)) return true;
         }
      }
      return false;
   }

   @Override
   public String toString() {
      return "ReplicateCommand{" +
            "commands=" + (commands == null ? "null" : Arrays.toString(commands)) +
            ", cacheName='" + cacheName + '\'' +
            '}';
   }
}