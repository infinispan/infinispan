/* 
 * JBoss, Home of Professional Open Source 
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tag. All rights reserved. 
 * See the copyright.txt in the distribution for a 
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use, 
 * modify, copy, or redistribute it subject to the terms and conditions 
 * of the GNU Lesser General Public License, v. 2.1. 
 * This program is distributed in the hope that it will be useful, but WITHOUT A 
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A 
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details. 
 * You should have received a copy of the GNU Lesser General Public License, 
 * v.2.1 along with this distribution; if not, write to the Free Software 
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, 
 * MA  02110-1301, USA.
 */
package org.infinispan.distexec.mapreduce;

import static org.infinispan.distexec.mapreduce.MapReduceTask.DEFAULT_TMP_CACHE_CONFIGURATION_NAME;
import static org.infinispan.factories.KnownComponentNames.ASYNC_TRANSPORT_EXECUTOR;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

import org.infinispan.Cache;
import org.infinispan.CacheException;
import org.infinispan.atomic.Delta;
import org.infinispan.atomic.DeltaAware;
import org.infinispan.commands.read.MapCombineCommand;
import org.infinispan.commands.read.ReduceCommand;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.distexec.mapreduce.spi.MapReduceTaskLifecycleService;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.loaders.CacheLoader;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.MarshalledValue;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.InfinispanCollections;
import org.infinispan.util.CollectionFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Default implementation of {@link MapReduceManager}.
 * <p>
 * 
 *  
 * This is an internal class, not intended to be used by clients.
 * @author Vladimir Blagojevic
 * @since 5.2
 */
public class MapReduceManagerImpl implements MapReduceManager {

   private static final Log log = LogFactory.getLog(MapReduceManagerImpl.class);
   private static final int CANCELLATION_CHECK_FREQUENCY = 20;
   private Address localAddress;
   private EmbeddedCacheManager cacheManager;
   private CacheLoaderManager cacheLoaderManager;
   private ExecutorService executorService;
   
   MapReduceManagerImpl() {
   }
   
   @Inject
   public void init(EmbeddedCacheManager cacheManager, CacheLoaderManager cacheLoaderManager,
            @ComponentName(ASYNC_TRANSPORT_EXECUTOR) ExecutorService asyncTransportExecutor) {
      this.cacheManager = cacheManager;
      this.cacheLoaderManager = cacheLoaderManager;
      this.localAddress = cacheManager.getAddress();
      this.executorService = asyncTransportExecutor;
   }
   
   @Override
   public ExecutorService getExecutorService() {
      return executorService;
   }

   @Override
   public <KIn, VIn, KOut, VOut> Map<KOut, List<VOut>> mapAndCombineForLocalReduction(
            MapCombineCommand<KIn, VIn, KOut, VOut> mcc) throws InterruptedException {
      CollectableCollector<KOut, VOut> collector = map(mcc);
      return combineForLocalReduction(mcc, collector);
   }

   @Override
   public <KIn, VIn, KOut, VOut> Set<KOut> mapAndCombineForDistributedReduction(
            MapCombineCommand<KIn, VIn, KOut, VOut> mcc) throws InterruptedException {
      CollectableCollector<KOut, VOut> collector = map(mcc);
      try {
         return combine(mcc, collector);
      } catch (Exception e) {
         throw new CacheException(e);
      }
   }

   @Override
   public <KOut, VOut> Map<KOut, VOut> reduce(ReduceCommand<KOut, VOut> reduceCommand)
            throws InterruptedException {
      Cache<?, ?> cache = cacheManager.getCache(reduceCommand.getCacheName());
      Set<KOut> keys = reduceCommand.getKeys();
      String taskId = reduceCommand.getTaskId();
      Reducer<KOut, VOut> reducer = reduceCommand.getReducer();
      boolean useIntermediateKeys = reduceCommand.isEmitCompositeIntermediateKeys();
      boolean noInputKeys = keys == null || keys.isEmpty();      
      Cache<Object, List<VOut>> tmpCache = cacheManager.getCache(reduceCommand.getCacheName());
      Map<KOut,VOut> result = new HashMap<KOut, VOut>();
      if (noInputKeys) {         
         //illegal state, raise exception
         throw new IllegalStateException("Reduce phase of MapReduceTask " + taskId + " on node "
                  + localAddress + " executed with empty input keys");
      } else{
         //first hook into lifecycle
         MapReduceTaskLifecycleService taskLifecycleService = MapReduceTaskLifecycleService.getInstance();
         log.tracef("For m/r task %s invoking %s at %s",  taskId, reduceCommand, localAddress);
         int interruptCount = 0;
         try {
            taskLifecycleService.onPreExecute(reducer, cache);
            for (KOut key : keys) {
               interruptCount++;
               if (checkInterrupt(interruptCount++) && Thread.currentThread().isInterrupted())
                  throw new InterruptedException();
               //load result value from map phase
               List<VOut> value;
               if(useIntermediateKeys) {
                  value = tmpCache.get(new IntermediateCompositeKey<KOut>(taskId, key));
               } else {
                  value = tmpCache.get(key);
               }               
               // and reduce it
               VOut reduced = reducer.reduce(key, value.iterator());
               result.put(key, reduced);
               log.tracef("For m/r task %s reduced %s to %s at %s ", taskId, key, reduced, localAddress);     
            }
         } finally {
            taskLifecycleService.onPostExecute(reducer);
         }
      }
      return result;
   }
   
   protected <KIn, VIn, KOut, VOut> CollectableCollector<KOut, VOut> map(
            MapCombineCommand<KIn, VIn, KOut, VOut> mcc) throws InterruptedException {
      Cache<KIn, VIn> cache = cacheManager.getCache(mcc.getCacheName());
      Set<KIn> keys = mcc.getKeys();
      Set<KIn> inputKeysCopy = null;
      Mapper<KIn, VIn, KOut, VOut> mapper = mcc.getMapper();
      DistributionManager dm = cache.getAdvancedCache().getDistributionManager();      
      boolean inputKeysSpecified = keys != null && !keys.isEmpty();
      Set <KIn> inputKeys = keys;      
      if (!inputKeysSpecified) {
         inputKeys = filterLocalPrimaryOwner(cache.keySet(), dm);
      } else {
         inputKeysCopy = new HashSet<KIn>(keys);
      }
      // hook map function into lifecycle and execute it
      MapReduceTaskLifecycleService taskLifecycleService = MapReduceTaskLifecycleService.getInstance();     
      DefaultCollector<KOut, VOut> collector = new DefaultCollector<KOut, VOut>();
      log.tracef("For m/r task %s invoking %s with input keys %s",  mcc.getTaskId(), mcc, inputKeys);
      int interruptCount = 0;
      try {
         taskLifecycleService.onPreExecute(mapper, cache);
         for (KIn key : inputKeys) {            
            if (checkInterrupt(interruptCount++) && Thread.currentThread().isInterrupted())
               throw new InterruptedException();
            
            VIn value = cache.get(key);
            mapper.map(key, value, collector);
            if (inputKeysSpecified) {
               inputKeysCopy.remove(key);
            }
         }
         Set<KIn> keysFromCacheLoader = null;
         if (inputKeysSpecified) {
            // load only specified remaining input keys - iff in CL and pinned to this primary owner
            keysFromCacheLoader = filterLocalPrimaryOwner(inputKeysCopy, dm);
         } else {
            // load everything from CL pinned to this primary owner
            keysFromCacheLoader = filterLocalPrimaryOwner(loadAllKeysFromCacheLoaderUsingFilter(inputKeys), dm);
         }   
         log.tracef("For m/r task %s cache loader input keys %s", mcc.getTaskId(), keysFromCacheLoader);
         interruptCount = 0;
         for (KIn key : keysFromCacheLoader) {                      
            if (checkInterrupt(interruptCount++) && Thread.currentThread().isInterrupted())
               throw new InterruptedException();
            
            VIn value = loadValueFromCacheLoader(key);            
            if(value != null){
               mapper.map(key, value, collector);
            }
         }
      } finally {
         taskLifecycleService.onPostExecute(mapper);
      }
      return collector;            
   }
   
   protected <KIn, VIn, KOut, VOut> Set<KOut> combine(MapCombineCommand<KIn, VIn, KOut, VOut> mcc,
            CollectableCollector<KOut, VOut> collector) throws Exception{
      
      String taskId =  mcc.getTaskId();      
      boolean emitCompositeIntermediateKeys = mcc.isEmitCompositeIntermediateKeys();                  
      Reducer <KOut,VOut> combiner = mcc.getCombiner();
      Set<KOut> mapPhaseKeys = new HashSet<KOut>();
      Cache<Object, DeltaAwareList<VOut>> tmpCache = null;
      if (emitCompositeIntermediateKeys) {
         tmpCache = cacheManager.getCache(DEFAULT_TMP_CACHE_CONFIGURATION_NAME);         
      } else {
         tmpCache = cacheManager.getCache(taskId);
      }
      if (tmpCache == null) {
         throw new IllegalStateException("Temporary cache for MapReduceTask " + taskId
                  + " not found on " + localAddress);
      }
      DistributionManager dm = tmpCache.getAdvancedCache().getDistributionManager();

      if (combiner != null) {
         Cache<?, ?> cache = cacheManager.getCache(mcc.getCacheName());
         log.tracef("For m/r task %s invoking combiner %s at %s",  taskId, mcc, localAddress);
         MapReduceTaskLifecycleService taskLifecycleService = MapReduceTaskLifecycleService.getInstance();
         Map<KOut, VOut> combinedMap = new ConcurrentHashMap<KOut, VOut>();
         try {
            taskLifecycleService.onPreExecute(combiner, cache);
            Map<KOut, List<VOut>> collectedValues = collector.collectedValues();
            for (Entry<KOut, List<VOut>> e : collectedValues.entrySet()) {
               List<VOut> list = e.getValue();
               VOut combined;
               if (list.size() > 1) {
                  combined = combiner.reduce(e.getKey(), list.iterator());
                  combinedMap.put(e.getKey(), combined);
               } else {
                  combined = list.get(0);
                  combinedMap.put(e.getKey(), combined);
               }               
               log.tracef("For m/r task %s combined %s to %s at %s" , taskId, e.getKey(), combined, localAddress);               
            }
         } finally {
            taskLifecycleService.onPostExecute(combiner);
         }
         Map<Address, List<KOut>> keysToNodes = mapKeysToNodes(dm, taskId, combinedMap.keySet(),
                  emitCompositeIntermediateKeys);

         for (Entry<Address, List<KOut>> entry : keysToNodes.entrySet()) {
            List<KOut> keysHashedToAddress = entry.getValue();
            try {
               log.tracef("For m/r task %s migrating intermediate keys %s to %s",  taskId, keysHashedToAddress, entry.getKey());
               for (KOut key : keysHashedToAddress) {
                  VOut value = combinedMap.get(key);
                  DeltaAwareList<VOut> delta = new DeltaAwareList<VOut>(value);
                  if (emitCompositeIntermediateKeys) {
                     tmpCache.put(new IntermediateCompositeKey<KOut>(taskId, key), delta);
                  } else {
                     tmpCache.put(key, delta);
                  }
                  mapPhaseKeys.add(key);
               }
            } catch (Exception e) {
               throw new CacheException("Could not move intermediate keys/values for M/R task " + taskId, e);
            }
         }
      } else {
         // Combiner not specified so lets insert each key/uncombined-List pair into tmp cache
         
         Map<KOut, List<VOut>> collectedValues = collector.collectedValues();         
         Map<Address, List<KOut>> keysToNodes = mapKeysToNodes(dm, taskId, collectedValues.keySet(),
                  emitCompositeIntermediateKeys);
                  
         for (Entry<Address, List<KOut>> entry : keysToNodes.entrySet()) {
            List<KOut> keysHashedToAddress = entry.getValue();
            try {
               log.tracef("For m/r task %s migrating intermediate keys %s to %s",  taskId, keysHashedToAddress, entry.getKey());
               for (KOut key : keysHashedToAddress) {
                  List<VOut> value = collectedValues.get(key);
                  DeltaAwareList<VOut> delta = new DeltaAwareList<VOut>(value);
                  if (emitCompositeIntermediateKeys) {
                     tmpCache.put(new IntermediateCompositeKey<KOut>(taskId, key), delta);
                  } else {
                     tmpCache.put(key, delta);
                  }
                  mapPhaseKeys.add(key);
               }
            } catch (Exception e) { 
               throw new CacheException("Could not move intermediate keys/values for M/R task " + taskId, e);
            }
         }
      }
      return mapPhaseKeys;
   }
   
   private <KIn, VIn, KOut, VOut> Map<KOut, List<VOut>> combineForLocalReduction(
            MapCombineCommand<KIn, VIn, KOut, VOut> mcc, 
            CollectableCollector<KOut, VOut> collector) {

      String taskId =  mcc.getTaskId();      
      Reducer <KOut,VOut> combiner = mcc.getCombiner();
      Map<KOut, List<VOut>> result = null;      

      if (combiner != null) {
         result = new HashMap<KOut, List<VOut>>();   
         log.tracef("For m/r task %s invoking combiner %s at %s",  taskId, mcc, localAddress);
         MapReduceTaskLifecycleService taskLifecycleService = MapReduceTaskLifecycleService.getInstance();
         try {
            Cache<?, ?> cache = cacheManager.getCache(mcc.getCacheName());
            taskLifecycleService.onPreExecute(combiner, cache);
            Map<KOut, List<VOut>> collectedValues = collector.collectedValues();
            for (Entry<KOut, List<VOut>> e : collectedValues.entrySet()) {
               VOut combined;
               List<VOut> list = e.getValue();               
               List<VOut> l = new LinkedList<VOut>();
               if (list.size() > 1) {
                  combined = combiner.reduce(e.getKey(), list.iterator());
               } else {
                  combined = list.get(0);                  
               }                              
               l.add(combined);
               result.put(e.getKey(), l);
               log.tracef("For m/r task %s combined %s to %s at %s" , taskId, e.getKey(), combined, localAddress);               
            }
         } finally {
            taskLifecycleService.onPostExecute(combiner);
         }
      } else {
         // Combiner not specified     
         result = collector.collectedValues();                  
      }
      return result;
   }
   
   private boolean checkInterrupt(int counter) {
      return counter % CANCELLATION_CHECK_FREQUENCY == 0;
   }
      
   @SuppressWarnings("unchecked")
   protected <KIn> Set<KIn> loadAllKeysFromCacheLoaderUsingFilter(Set<KIn> filterOutSet) {      
      Set<KIn> keysInCL = InfinispanCollections.<KIn>emptySet();
      CacheLoader cl = resolveCacheLoader();
      if (cl != null) {
         try {
            keysInCL = (Set<KIn>) cl.loadAllKeys((Set<Object>) filterOutSet);
         } catch (CacheLoaderException e) {
            throw new CacheException("Could not load key/value entries from cacheloader", e);
         }
      }
      return keysInCL;
   }
   
   @SuppressWarnings("unchecked")
   protected <KIn, KOut> KOut loadValueFromCacheLoader(KIn key) {      
      KOut value = null;
      CacheLoader cl = resolveCacheLoader();
      if (cl != null) {
         try {
            InternalCacheEntry entry = cl.load(key);
            if (entry != null) {               
               Object loadedValue = entry.getValue();
               if (loadedValue instanceof MarshalledValue) {
                  value = (KOut) ((MarshalledValue) loadedValue).get();
               } else {
                  value = (KOut) loadedValue;
               }
            }
         } catch (CacheLoaderException e) {
            throw new CacheException("Could not load key/value entries from cacheloader", e);
         }
      }
      return value;
   }
   
   protected CacheLoader resolveCacheLoader(){
      CacheLoader cl = null;
      if (cacheLoaderManager != null && cacheLoaderManager.isEnabled()){ 
         cl = cacheLoaderManager.getCacheLoader();
      }
      return cl;
   }
   
   public <T> Map<Address, List<T>> mapKeysToNodes(DistributionManager dm, String taskId,
            Collection<T> keysToMap, boolean useIntermediateCompositeKey) {
      Map<Address, List<T>> addressToKey = new HashMap<Address, List<T>>();
      for (T key : keysToMap) {
         Address ownerOfKey = null;
         if (useIntermediateCompositeKey) {
            ownerOfKey = dm.getPrimaryLocation(new IntermediateCompositeKey<T>(taskId, key));
         } else {
            ownerOfKey = dm.getPrimaryLocation(key);
         }
         List<T> keysAtNode = addressToKey.get(ownerOfKey);
         if (keysAtNode == null) {
            keysAtNode = new ArrayList<T>();
            addressToKey.put(ownerOfKey, keysAtNode);
         }
         keysAtNode.add(key);
      }
      return addressToKey;
   }
   
   protected <KIn> Set<KIn> filterLocalPrimaryOwner(Set<KIn> nodeLocalKeys, DistributionManager dm) {    
      Set<KIn> selectedKeys = new HashSet<KIn>();
      for (KIn key : nodeLocalKeys) {
         Address primaryLocation = dm.getPrimaryLocation(key);
         if (primaryLocation != null && primaryLocation.equals(localAddress)) {
            selectedKeys.add(key);
         }
      }
      return selectedKeys;
   }
   
   /**
    * @author Sanne Grinovero <sanne@hibernate.org> (C) 2011 Red Hat Inc.
    */
   private static class DefaultCollector<KOut, VOut> implements CollectableCollector<KOut, VOut> {

      private final Map<KOut, List<VOut>> store = CollectionFactory.makeConcurrentMap();

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
         this.clear();
      }
      
      @SuppressWarnings("unchecked")
      @Override
      public DeltaAware merge(DeltaAware d) {
         List<E> other = null;
         if (d != null && d instanceof DeltaAwareList) {
            other = (List<E>) d;
            for (E e : this) {
               other.add(e);
            }            
            return (DeltaAware) other;
         } else {
            return this;
         }      
      }
   }
   
   /**
    * IntermediateCompositeKey
    * 
    */
   public static final class IntermediateCompositeKey<V> implements Serializable {

      /** The serialVersionUID */
      private static final long serialVersionUID = 4434717760740027918L;
      
      private final String taskId;
      private final V key;

      public IntermediateCompositeKey(String taskId, V key) {
         this.taskId = taskId;
         this.key = key;
      }

      @Override
      public int hashCode() {
         final int prime = 31;
         int result = 1;
         result = prime * result + ((key == null) ? 0 : key.hashCode());
         result = prime * result + ((taskId == null) ? 0 : taskId.hashCode());
         return result;
      }

      @SuppressWarnings("unchecked")
      @Override
      public boolean equals(Object obj) {     
         if (obj == null) {
            return false;
         }
         if (!(obj instanceof IntermediateCompositeKey)) {
            return false;
         }
         IntermediateCompositeKey<V> other = (IntermediateCompositeKey<V>) obj;
         if (key == null) {
            if (other.key != null) {
               return false;
            }
         } else if (!key.equals(other.key)) {
            return false;
         }
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
         return "IntermediateCompositeKey [taskId=" + taskId + ", key=" + key + "]";
      }
   }
}
