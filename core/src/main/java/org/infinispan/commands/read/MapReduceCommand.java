/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
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

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.distexec.mapreduce.Collector;
import org.infinispan.distexec.mapreduce.Mapper;
import org.infinispan.distexec.mapreduce.Reducer;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * MapReduceCommand is used to migrate {@link Mapper} and {@link Reducer} to remote JVM where they
 * will get invoked.
 * 
 * @author Vladimir Blagojevic
 * @since 5.0
 */
public class MapReduceCommand extends BaseRpcCommand {
   public static final int COMMAND_ID = 20;
   private static final Log log = LogFactory.getLog(MapReduceCommand.class);
   protected Set<Object> keys;
   private Mapper  mapper;
   private Reducer  reducer;
   
   private InterceptorChain invoker;
   private CommandsFactory commandsFactory;
   protected InvocationContextContainer icc;
   protected DistributionManager dm;
   protected Address localAddress;

   private MapReduceCommand() {
      super(null); // For command id uniqueness test
   }

   public MapReduceCommand(String cacheName) {
      super(cacheName);
   }

   public MapReduceCommand(Mapper m, Reducer r, String cacheName, Object... inputKeys) {
      super(cacheName);
      if (inputKeys == null || inputKeys.length == 0) {
         this.keys = new HashSet<Object>();
      } else {
         this.keys = new HashSet<Object>();
         this.keys.addAll(Arrays.asList(inputKeys));
      }
      this.mapper = m;
      this.reducer = r;
   }

   public MapReduceCommand(Mapper m, Reducer r, String cacheName, Collection<Object> inputKeys) {
      super(cacheName);
      if (inputKeys == null || inputKeys.isEmpty())
         this.keys = new HashSet<Object>();
      else
         this.keys = new HashSet<Object>(inputKeys);
     
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
    * Performs invocation of mapping phase and local reduce phase before returning result to master node
    * 
    * @param context
    *           invocation context
    * @return Map of intermediate key value pairs
    */
   @Override
   public Object perform(InvocationContext context) throws Throwable {
      InvocationContext ctx = getInvocationContext(context);
      // find targets keys and invoked Mapper on them
      boolean noInputKeys = keys == null || keys.isEmpty();
      if (noInputKeys) {         
         KeySetCommand keySetCommand = commandsFactory.buildKeySetCommand();
         Set<Object> nodeLocalKeys = (Set<Object>) invoker.invoke(ctx, keySetCommand);
         List<Object> selectedKeys = new ArrayList<Object>();
         for (Object key : nodeLocalKeys) {
            Address primaryLocation = dm.getPrimaryLocation(key);
            log.tracef("For key %s at %s owner is %s", key, localAddress, primaryLocation);
            if (primaryLocation != null && primaryLocation.equals(localAddress)) {
               selectedKeys.add(key);
            }
         }
         if (keys == null)
            keys = new HashSet<Object>();

         keys.addAll(selectedKeys);
      }
      log.tracef("For %s at %s invoking mapper on keys %s", this, localAddress, keys);
      DefaultCollector<Object,Object> collector = new DefaultCollector<Object, Object>();
      for (Object key : keys) {
         GetKeyValueCommand command = commandsFactory.buildGetKeyValueCommand(key, ctx.getFlags());
         command.setReturnCacheEntry(false);
         Object value = invoker.invoke(ctx, command);
         mapper.map(key, value,collector);
      }
      Map<Object, List<Object>> collectedValues = collector.collectedValues();
      Map<Object,Object> reducedMap = new HashMap<Object, Object>();
      for (Entry<Object, List<Object>> e : collectedValues.entrySet()) {
         List<Object> list = e.getValue();
         if(list.size()>1){
            Object reduced = reducer.reduce(e.getKey(),list.iterator());
            reducedMap.put(e.getKey(), reduced);
         } else {
            reducedMap.put(e.getKey(),list.get(0));
         }
      }                 
      log.tracef("%s executed at %s was reduced to %s", this, localAddress, reducedMap);
      return reducedMap;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public Object[] getParameters() {
      return new Object[] {keys, mapper, reducer};
   }

   @Override
   public void setParameters(int commandId, Object[] args) {
      if (commandId != COMMAND_ID)
         throw new IllegalStateException("Invalid method id");
      int i = 0;
      keys = (Set<Object>) args[i++];
      mapper = (Mapper)args[i++];
      reducer = (Reducer) args[i++];
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
      if (keys.equals(that.keys)) {
         return false;
      }
      if (mapper != null && reducer != null && that.mapper != null && that.reducer != null) {
         return mapper.getClass().equals(that.mapper.getClass())
                  && reducer.getClass().equals(that.reducer.getClass());
      }
      return false;
   }

   @Override
   public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + (keys != null ? keys.hashCode() : 0);
      result = 31 * result + (mapper != null ? mapper.getClass().hashCode() : 0);
      result = 31 * result + (reducer != null ? reducer.getClass().hashCode() : 0);
      return result;
   }
   
   @Override
   public String toString() {
      return "MapReduceCommand(keys=" + keys+")";
   }
   
   private InvocationContext getInvocationContext(InvocationContext ctx) {
      return  ctx == null ? icc.createRemoteInvocationContext(localAddress):ctx;      
   }
   
   /**
    * @author Sanne Grinovero <sanne@hibernate.org> (C) 2011 Red Hat Inc.
    */
   private static class DefaultCollector<KOut, VOut> implements Collector<KOut, VOut> {

      private final Map<KOut, List<VOut>> store = new ConcurrentHashMap<KOut, List<VOut>>();

      @Override
      public void emit(KOut key, VOut value) {
         List<VOut> list = store.get(key);
         if (list == null) {
            list = new LinkedList<VOut>();
            store.put(key, list);
         }
         list.add(value);
      }

      public Map<KOut, List<VOut>> collectedValues() {
         return store;
      }
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }
}