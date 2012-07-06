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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.infinispan.Cache;
import org.infinispan.atomic.Delta;
import org.infinispan.atomic.DeltaAware;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.distexec.mapreduce.Collector;
import org.infinispan.distexec.mapreduce.Mapper;
import org.infinispan.distexec.mapreduce.Reducer;
import org.infinispan.distexec.mapreduce.spi.MapReduceTaskLifecycleService;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.concurrent.ConcurrentMapFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * MapCombineCommand is a container to migrate {@link Mapper} and {@link Reducer} which is a
 * combiner to a remote Infinispan node where it will get executed and return the result to an
 * invoking/master node.
 * 
 * @author Vladimir Blagojevic
 * @since 5.0
 */
public class MapCombineCommand extends BaseRpcCommand {
   public static final int COMMAND_ID = 30;
   private static final Log log = LogFactory.getLog(MapCombineCommand.class);
   private Set<Object> keys = new HashSet<Object>();
   private Mapper  mapper;
   private Reducer  combiner;
   
   private InterceptorChain invoker;
   private CommandsFactory commandsFactory;
   private InvocationContextContainer icc;
   private DistributionManager dm;
   private Address localAddress;
   private EmbeddedCacheManager cacheManager;
   private String taskId;

   private MapCombineCommand() {
      super(null); // For command id uniqueness test
   }

   public MapCombineCommand(String cacheName) {
      super(cacheName);
   }

   public MapCombineCommand(String taskId, Mapper mapper, Reducer combiner, String cacheName, Collection<Object> inputKeys) {
      super(cacheName);
      this.taskId = taskId;
      if (inputKeys != null && !inputKeys.isEmpty()){
         keys.addAll(inputKeys);
      }      
      this.mapper = mapper;
      this.combiner = combiner;
   }
   
   public void init(CommandsFactory factory, InterceptorChain invoker,
            InvocationContextContainer icc, DistributionManager dm, EmbeddedCacheManager cacheManager, Address localAddress) {
      this.commandsFactory = factory;
      this.invoker = invoker;
      this.icc = icc;
      this.dm = dm;      
      this.localAddress = localAddress;
      this.cacheManager = cacheManager;
   }

   /**
    * Performs invocation of mapping phase and local combine phase on assigned Infinispan node
    * 
    * @param context
    *           invocation context
    * @return Map of intermediate key value pairs
    */
   @Override
   public Object perform(InvocationContext context) throws Throwable {
      CollectableCollector<Object, Object> collector = map(context);
      return combine(collector);
   }

   @SuppressWarnings("unchecked")
   private <KOut,VOut> Set<KOut> combine(CollectableCollector<KOut, VOut> collector) {      
      Cache<Object, Object> tmpCache = cacheManager.getCache(taskId);
      if (tmpCache == null) {
         throw new IllegalStateException("Temporary cache for MapReduceTask " + taskId
                  + " not found on " + localAddress);
      }           
      Set<KOut> mapPhaseKeys = new HashSet<KOut>();
      if (combiner != null) {
         MapReduceTaskLifecycleService taskLifecycleService = MapReduceTaskLifecycleService.getInstance();
         Map<KOut, VOut> combinedMap = new HashMap<KOut, VOut>();
         try {
            taskLifecycleService.onPreExecute(combiner);
            Map<KOut, List<VOut>> collectedValues = collector.collectedValues();
            for (Entry<KOut, List<VOut>> e : collectedValues.entrySet()) {
               List<VOut> list = e.getValue();
               if (list.size() > 1) {
                  VOut combined = (VOut) combiner.reduce(e.getKey(), list.iterator());
                  combinedMap.put(e.getKey(), combined);
               } else {
                  combinedMap.put(e.getKey(), list.get(0));
               }
            }
         } finally {
            taskLifecycleService.onPostExecute(combiner);
         }
         log.tracef("%s executed at %s was combined to %s", this, localAddress, combinedMap);

         // finally insert each key/combined-value pair into designated tmp cache
         for (Entry<KOut, VOut> e : combinedMap.entrySet()) {
            KOut key = e.getKey();
            VOut combinedValue = e.getValue();
            DeltaAwareList<VOut> delta = new DeltaAwareList<VOut>(combinedValue);
            tmpCache.put(key, delta);
            mapPhaseKeys.add(key);
         }         
      } else {
         // Combiner not specified so lets insert each key/uncombined-List pair into tmp cache
         Map<KOut, List<VOut>> collectedValues = collector.collectedValues();         
         for (Entry<KOut, List<VOut>> e : collectedValues.entrySet()) {
            KOut key = e.getKey();
            List<VOut> uncombinedList = e.getValue();
            DeltaAwareList<VOut> delta = new DeltaAwareList<VOut>(uncombinedList);
            tmpCache.put(key, delta);
            mapPhaseKeys.add(key);
         }
      }
      return mapPhaseKeys;
   }
   
   @SuppressWarnings("unchecked")
   protected <KOut,VOut> CollectableCollector<KOut, VOut> map(InvocationContext context){
      InvocationContext ctx = getInvocationContext(context);
      // find targets keys and invoked Mapper on them
      boolean noInputKeys = keys == null || keys.isEmpty();
      if (noInputKeys) {
         List<Object> selectedKeys = executeKeySetCommand(ctx);
         keys.addAll(selectedKeys);
      }
      // hook map function into lifecycle and execute it
      MapReduceTaskLifecycleService taskLifecycleService = MapReduceTaskLifecycleService.getInstance();
      log.tracef("For %s at %s invoking mapper on keys %s", this, localAddress, keys);
      DefaultCollector<KOut, VOut> collector = new DefaultCollector<KOut, VOut>();
      try {
         taskLifecycleService.onPreExecute(mapper);
         for (Object key : keys) {
            GetKeyValueCommand command = commandsFactory.buildGetKeyValueCommand(key, ctx.getFlags());
            command.setReturnCacheEntry(false);
            Object value = invoker.invoke(ctx, command);
            mapper.map(key, value, collector);
         }
      } finally {
         taskLifecycleService.onPostExecute(mapper);
      }
      return collector;            
   }

   @SuppressWarnings("unchecked")
   private List<Object> executeKeySetCommand(InvocationContext ctx) {
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
      return selectedKeys;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public Object[] getParameters() {
      return new Object[] {taskId, keys, mapper, combiner};
   }

   @SuppressWarnings("unchecked")
   @Override
   public void setParameters(int commandId, Object[] args) {
      if (commandId != COMMAND_ID)
         throw new IllegalStateException("Invalid method id");
      int i = 0;
      taskId = (String) args[i++];
      keys = (Set<Object>) args[i++];
      mapper = (Mapper)args[i++];
      combiner = (Reducer) args[i++];
   }
   
   private InvocationContext getInvocationContext(InvocationContext ctx) {
      return  ctx == null ? icc.createRemoteInvocationContext(localAddress):ctx;      
   }
   
   @Override
   public boolean isReturnValueExpected() {
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
   public boolean equals(Object obj) {
      if (this == obj) {
         return true;
      }
      if (obj == null) {
         return false;
      }
      if (!(obj instanceof MapCombineCommand)) {
         return false;
      }
      MapCombineCommand other = (MapCombineCommand) obj;
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
   public String toString() {
      return "MapCombineCommand [keys=" + keys + ", localAddress=" + localAddress + ", taskId="
               + taskId + "]";
   }
   
   /**
    * @author Sanne Grinovero <sanne@hibernate.org> (C) 2011 Red Hat Inc.
    */
   private static class DefaultCollector<KOut, VOut> implements CollectableCollector<KOut, VOut> {

      private final Map<KOut, List<VOut>> store = ConcurrentMapFactory.makeConcurrentMap();

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
   
   private interface CollectableCollector<K,V> extends Collector<K, V>{      
      Map<K, List<V>> collectedValues();
   }
   
   private static class DeltaAwareList<E> extends LinkedList<E> implements DeltaAware, Delta{
            
     
      /** The serialVersionUID */
      private static final long serialVersionUID = 2176345973026460708L;

      public DeltaAwareList() {
         super();
      }

      public DeltaAwareList(Collection<? extends E> c) {
         super(c);
      }

      public DeltaAwareList(E reducedObject) {
         super();
         add(reducedObject);
      }

      @Override
      public Delta delta() {
         return new DeltaAwareList<E>(this);
      }

      @Override
      public void commit() {
         // nothing TODO
      }
      
      @Override
      public DeltaAware merge(DeltaAware d) {
         List<E> other = null;
         if (d != null && d instanceof DeltaAwareList) {
            other = (List) d;
            for (E e : this) {
               other.add(e);
            }            
            return (DeltaAware) other;
         } else {
            return this;
         }      
      }
   }
}