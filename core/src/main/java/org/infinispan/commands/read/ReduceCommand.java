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
package org.infinispan.commands.read;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.distexec.mapreduce.Reducer;
import org.infinispan.distexec.mapreduce.spi.MapReduceTaskLifecycleService;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * ReduceCommand is a container to migrate {@link Reducer} to a remote Infinispan node where it will
 * get executed and return the result to an invoking/master node.
 * 
 * @author Vladimir Blagojevic
 * @since 5.2
 */
public class ReduceCommand extends BaseRpcCommand {
   
   public static final int COMMAND_ID = 31;
   private static final Log log = LogFactory.getLog(ReduceCommand.class);
   private Set<Object> keys = new HashSet<Object>();
   private Reducer reducer;
   
   private InterceptorChain invoker;
   private CommandsFactory commandsFactory;
   private InvocationContextContainer icc;
   private Address localAddress;
   private String taskId;

   private ReduceCommand() {
      super(null); // For command id uniqueness test
   }

   public ReduceCommand(String cacheName) {
      super(cacheName);
   }

   public ReduceCommand(String taskId, Reducer reducer, String cacheName, Collection<Object> inputKeys) {
      super(cacheName);
      this.taskId = taskId;
      if (inputKeys != null && !inputKeys.isEmpty()){
         keys.addAll(inputKeys);
      }      
      this.reducer = reducer;
   }
   
   public void init(CommandsFactory factory, InterceptorChain invoker,
            InvocationContextContainer icc, Address localAddress) {
      this.commandsFactory = factory;
      this.invoker = invoker;
      this.icc = icc;
      this.localAddress = localAddress;
   }

   /**
    * Performs invocation of reduce phase on assigned Infinispan node
    * 
    * @param context
    *           invocation context
    * @return Map of intermediate key value pairs
    */
   @SuppressWarnings("unchecked")
   @Override
   public Object perform(InvocationContext context) throws Throwable {
      InvocationContext ctx = getInvocationContext(context);
      // find targets keys
      boolean noInputKeys = keys == null || keys.isEmpty();
      Map<Object,Object> result = new HashMap<Object, Object>();
      if (noInputKeys) {         
         //illegal state, raise exception
         throw new IllegalStateException("Reduce phase of MapReduceTask " + taskId + " on node "
                  + localAddress + " executed with empty input keys");
      } else{
         //first hook into lifecycle
         MapReduceTaskLifecycleService taskLifecycleService = MapReduceTaskLifecycleService.getInstance();
         log.tracef("For %s at %s invoking mapper on keys %s", this, localAddress, keys);
         try {
            taskLifecycleService.onPreExecute(reducer);         
            for (Object key : keys) {
               GetKeyValueCommand command = commandsFactory.buildGetKeyValueCommand(key, ctx.getFlags());
               command.setReturnCacheEntry(false);
               //load result value from map phase
               List<Object> value = (List<Object>) invoker.invoke(ctx, command);
               // and reduce it
               Object reduced = reducer.reduce(key, value.iterator());
               result.put(key, reduced);
            }
         } finally {
            taskLifecycleService.onPostExecute(reducer);
         }
      }
      return result;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public Object[] getParameters() {
      return new Object[] {taskId, keys, reducer};
   }

   @SuppressWarnings({ "unchecked", "rawtypes" })
   @Override
   public void setParameters(int commandId, Object[] args) {
      if (commandId != COMMAND_ID)
         throw new IllegalStateException("Invalid method id");
      int i = 0;
      taskId = (String) args[i++];
      keys = (Set<Object>) args[i++];
      reducer = (Reducer) args[i++];
   }
   
   private InvocationContext getInvocationContext(InvocationContext ctx) {
      return  ctx == null ? icc.createRemoteInvocationContext(localAddress):ctx;      
   }
   
   @Override
   public boolean isReturnValueExpected() {
      return true;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj) {
         return true;
      }
      if (obj == null) {
         return false;
      }
      if (!(obj instanceof ReduceCommand)) {
         return false;
      }
      ReduceCommand other = (ReduceCommand) obj;
      if (taskId == null) {
         if (other.taskId != null) {
            return false;
         }
      } else if (!taskId.equals(other.taskId)) {
         return false;
      }
      return true;
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((taskId == null) ? 0 : taskId.hashCode());
      return result;
   }
   
   @Override
   public String toString() {
      return "ReduceCommand [keys=" + keys + ", localAddress=" + localAddress + ", taskId="
               + taskId + "]";
   }
}