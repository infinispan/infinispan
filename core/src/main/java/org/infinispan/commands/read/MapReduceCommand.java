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
package org.infinispan.commands.read;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.distexec.mapreduce.Mapper;
import org.infinispan.distexec.mapreduce.Reducer;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * 
 */
public class MapReduceCommand extends BaseRpcCommand {
   public static final int COMMAND_ID = 20;
   private static final Log log = LogFactory.getLog(MapReduceCommand.class);
   protected Object[] keys;
   protected String blah;
   private Mapper  mapper;
   private Reducer  reducer;
   
   private InterceptorChain invoker;
   private CommandsFactory commandsFactory;
   protected InvocationContextContainer icc;
   protected DistributionManager dm;
   protected Address localAddress;

   public MapReduceCommand() {
   }

   public MapReduceCommand( Mapper m, Reducer r, String cacheName, Object... inputKeys) {
      super(cacheName);
      if (inputKeys == null || inputKeys.length == 0) {
         this.keys = new Object[] {};
      } else {
         this.keys = inputKeys;
      }
      this.mapper = m;
      this.reducer = r;
   }

   public MapReduceCommand( Mapper m, Reducer r, String cacheName, Collection<Object> inputKeys) {
      super(cacheName);
      if (inputKeys == null || inputKeys.isEmpty())
         this.keys = new Object[] {};
      else
         this.keys = inputKeys.toArray(new Object[inputKeys.size()]);
      
      this.mapper = m;
      this.reducer = r;
   }
   
   public void init(CommandsFactory factory, InterceptorChain invoker,
            InvocationContextContainer icc, DistributionManager dm, Address localAddress) {
      this.commandsFactory = factory;
      this.invoker = invoker;
      this.icc = icc;
      this.dm = dm;      
      this.localAddress = localAddress;
   }

   /**
    * Performs an invalidation on a specified entry
    * 
    * @param ctx
    *           invocation context
    * @return null
    */
   @Override
   public Object perform(InvocationContext context) throws Throwable {
      InvocationContext ctx = getInvocationContext(context);
      // find targets keys and invoked Mapper on them
      boolean noInputKeys = keys == null || keys.length == 0;
      if (noInputKeys) {         
         KeySetCommand keySetCommand = commandsFactory.buildKeySetCommand();
         Set<Object> nodeLocalKeys = (Set<Object>) invoker.invoke(ctx, keySetCommand);
         List<Object> selectedKeys = new ArrayList<Object>();
         for (Object key : nodeLocalKeys) {
            List<Address> locations = dm.locate(key);
            log.trace("For key %s at %s owners are %s", key, localAddress, locations);
            if(locations != null && !locations.isEmpty() && locations.get(0).equals(localAddress)){
               selectedKeys.add(key);               
            }
         }
         keys = selectedKeys.toArray(new Object[selectedKeys.size()]);
      }
      log.trace("For %s at %s invoking mapper on keys %s",this,localAddress, Arrays.toString(keys));
      Object previouslyReduced = null;
      for (Object key : keys) {
         GetKeyValueCommand command = commandsFactory.buildGetKeyValueCommand(key, ctx.getFlags());
         Object value = invoker.invoke(ctx, command);
         Object mapResult = mapper.map(key, value);
         previouslyReduced = reducer.reduce(mapResult, previouslyReduced);         
      }
      log.trace("%s executed at %s was reduced to %s", this, localAddress, previouslyReduced);
      return previouslyReduced;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public Object[] getParameters() {
      Object[] retval = new Object[] { 0 };
      if (keys != null) {
         if (keys.length == 1) {
            retval = new Object[] { 1, keys[0] };
         } else {
            retval = new Object[keys.length + 1];
            retval[0] = keys.length;
            System.arraycopy(keys, 0, retval, 1, keys.length);
         }
      }
      return new Object[] { cacheName, retval, mapper, reducer};
   }

   @Override
   public void setParameters(int commandId, Object[] args) {
      if (commandId != COMMAND_ID)
         throw new IllegalStateException("Invalid method id");
      int i = 0;
      cacheName = (String) args[i++];
      Object[] k = (Object[]) args[i++];
      int size = (Integer) k[0];
      keys = new Object[size];
      if (size == 1) {
         keys[0] = k[1];
      } else if (size > 0) {
         System.arraycopy(args, 1, keys, 0, size);
      }
      mapper = (Mapper)args[i++];
      reducer = (Reducer) args[i++];
   }

   public Object[] getKeys() {
      return keys;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) {
         return true;
      }
      if (!(o instanceof MapReduceCommand)) {
         return false;
      }
      if (!super.equals(o)) {
         return false;
      }

      MapReduceCommand that = (MapReduceCommand) o;

      if (!Arrays.equals(keys, that.keys)) {
         return false;
      }
      return true;
   }

   @Override
   public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + (keys != null ? Arrays.hashCode(keys) : 0);
      return result;
   }
   
   @Override
   public String toString() {
      return "MapReduceCommand(keys=" + Arrays.toString(keys) +')';
   }
   
   private InvocationContext getInvocationContext(InvocationContext ctx) {
      return  ctx == null ? icc.createRemoteInvocationContext():ctx;      
   }
}