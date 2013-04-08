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
package org.infinispan.distexec.mapreduce;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.CacheException;
import org.infinispan.commands.CancelCommand;
import org.infinispan.commands.CancellationService;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.CreateCacheCommand;
import org.infinispan.commands.read.MapCombineCommand;
import org.infinispan.commands.read.ReduceCommand;
import org.infinispan.distexec.mapreduce.spi.MapReduceTaskLifecycleService;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.Marshaller;
import org.infinispan.marshall.StreamingMarshaller;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.Util;
import org.infinispan.util.concurrent.AbstractInProcessFuture;
import org.infinispan.util.concurrent.FutureListener;
import org.infinispan.util.concurrent.NotifyingFuture;
import org.infinispan.util.concurrent.NotifyingNotifiableFuture;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.infinispan.factories.KnownComponentNames.CACHE_MARSHALLER;

/**
 * MapReduceTask is a distributed task allowing a large scale computation to be transparently
 * parallelized across Infinispan cluster nodes.
 * <p>
 * 
 * Users should instantiate MapReduceTask with a reference to a cache whose data is used as input for this
 * task. Infinispan execution environment will migrate and execute instances of provided {@link Mapper} 
 * and {@link Reducer} seamlessly across Infinispan nodes.
 * <p>
 * 
 * Unless otherwise specified using {@link MapReduceTask#onKeys(Object...)} filter all available 
 * key/value pairs of a specified cache will be used as input data for this task.
 * 
 * For example, MapReduceTask that counts number of word occurrences in a particular cache where 
 * keys and values are String instances could be written as follows:
 *  
 * <pre>
 * MapReduceTask&lt;String, String, String, Integer&gt; task = new MapReduceTask&lt;String, String, String, Integer&gt;(cache);
 * task.mappedWith(new WordCountMapper()).reducedWith(new WordCountReducer());
 * Map&lt;String, Integer&gt; results = task.execute();
 * </pre>
 * 
 * The final result is a map where key is a word and value is a word count for that particular word.
 * <p>
 * 
 * Accompanying {@link Mapper} and {@link Reducer} are defined as follows: 
 * 
 * <pre>
 *    private static class WordCountMapper implements Mapper&lt;String, String, String,Integer&gt; {
 *     
 *     public void map(String key, String value, Collector&lt;String, Integer&gt; collector) {
 *        StringTokenizer tokens = new StringTokenizer(value);
 *       while (tokens.hasMoreElements()) {
 *           String s = (String) tokens.nextElement();
 *           collector.emit(s, 1);
 *        }         
 *     }
 *  }
 *
 *   private static class WordCountReducer implements Reducer&lt;String, Integer&gt; {
 *     
 *      public Integer reduce(String key, Iterator&lt;Integer&gt; iter) {
 *         int sum = 0;
 *         while (iter.hasNext()) {
 *            Integer i = (Integer) iter.next();
 *            sum += i;
 *        }
 *         return sum;
 *      }
 *   }
 * </pre>
 * 
 * <p>
 * 
 * Finally, as of Infinispan 5.2 release, MapReduceTask can also specify a Combiner function. The Combiner 
 * is executed on each node after the Mapper and before the global reduce phase. The Combiner receives input from 
 * the Mapper's output and the output from the Combiner is then sent to the reducers. It is useful to think 
 * of the Combiner as a node local reduce phase before global reduce phase is executed.
 * <p>
 * 
 * Combiners are especially useful when reduce function is both commutative and associative! In such cases 
 * we can use the Reducer itself as the Combiner; all one needs to do is to specify the Combiner:
 * <pre>
 * MapReduceTask&lt;String, String, String, Integer&gt; task = new MapReduceTask&lt;String, String, String, Integer&gt;(cache);
 * task.mappedWith(new WordCountMapper()).reducedWith(new WordCountReducer()).combineWith(new WordCountReducer());
 * Map&lt;String, Integer&gt; results = task.execute();
 * </pre>
 * 
 * Note that {@link Mapper} and {@link Reducer} should not be specified as inner classes. Inner classes 
 * declared in non-static contexts contain implicit non-transient references to enclosing class instances, 
 * serializing such an inner class instance will result in serialization of its associated outer class instance as well.
 * 
 * <p>
 * 
 * If you are not familiar with concept of map reduce distributed execution model 
 * start with Google's MapReduce research <a href="http://labs.google.com/papers/mapreduce.html">paper</a>.
 * 
 * 
 * @author Manik Surtani
 * @author Vladimir Blagojevic
 * @author Sanne Grinovero
 * 
 * @since 5.0
 */
public class MapReduceTask<KIn, VIn, KOut, VOut> {

   private static final Log log = LogFactory.getLog(MapReduceTask.class);
   public static final String DEFAULT_TMP_CACHE_CONFIGURATION_NAME= "__tmpMapReduce";

   protected Mapper<KIn, VIn, KOut, VOut> mapper;
   protected Reducer<KOut, VOut> reducer;
   protected Reducer<KOut, VOut> combiner;
   protected final boolean distributeReducePhase;
   protected final boolean useIntermediateSharedCache;

   protected final Collection<KIn> keys;
   protected final AdvancedCache<KIn, VIn> cache;
   protected final Marshaller marshaller;
   protected final MapReduceManager mapReduceManager;
   protected final CancellationService cancellationService;
   protected final List<CancellableTaskPart> cancellableTasks;
   protected final UUID taskId;

   /**
    * Create a new MapReduceTask given a master cache node. All distributed task executions will be
    * initiated from this cache node. This task will by default only use distributed map phase while
    * reduction will be executed on task originating Infinispan node.
    * <p>
    * 
    * Large and data intensive tasks whose reduction phase would exceed working memory of one
    * Infinispan node should use distributed reduce phase
    * 
    * @param masterCacheNode
    *           cache node initiating map reduce task
    */
   public MapReduceTask(Cache<KIn, VIn> masterCacheNode) {
      this(masterCacheNode, false, false);
   }
   
   /**
    * Create a new MapReduceTask given a master cache node. All distributed task executions will be
    * initiated from this cache node.
    * 
    * @param masterCacheNode
    *           cache node initiating map reduce task
    * @param distributeReducePhase
    *           if true this task will use distributed reduce phase execution
    * 
    */
   public MapReduceTask(Cache<KIn, VIn> masterCacheNode, boolean distributeReducePhase) {
      this(masterCacheNode, distributeReducePhase, true);
   }

   
   /**
    * Create a new MapReduceTask given a master cache node. All distributed task executions will be
    * initiated from this cache node.
    * 
    * @param masterCacheNode
    *           cache node initiating map reduce task
    * @param distributeReducePhase
    *           if true this task will use distributed reduce phase execution
    * @param useIntermediateSharedCache
    *           if true this tasks will share intermediate value cache with other executing
    *           MapReduceTasks on the grid. Otherwise, if false, this task will use its own
    *           dedicated cache for intermediate values
    * 
    */
   public MapReduceTask(Cache<KIn, VIn> masterCacheNode, boolean distributeReducePhase, boolean useIntermediateSharedCache) {
      if (masterCacheNode == null)
         throw new IllegalArgumentException("Can not use null cache for MapReduceTask");

      ensureProperCacheState(masterCacheNode.getAdvancedCache());      
      this.cache = masterCacheNode.getAdvancedCache();
      this.keys = new LinkedList<KIn>();
      this.marshaller = cache.getComponentRegistry().getComponent(StreamingMarshaller.class, CACHE_MARSHALLER);
      this.mapReduceManager = cache.getComponentRegistry().getComponent(MapReduceManager.class);
      this.cancellationService = cache.getComponentRegistry().getComponent(CancellationService.class);
      this.taskId = UUID.randomUUID();
      this.distributeReducePhase = distributeReducePhase;  
      this.useIntermediateSharedCache = useIntermediateSharedCache;
      this.cancellableTasks = Collections.synchronizedList(new ArrayList<CancellableTaskPart>());
   }

   /**
    * Rather than use all available keys as input <code>onKeys</code> allows users to specify a
    * subset of keys as input to this task
    * 
    * @param input
    *           input keys for this task
    * @return this task
    */
   public MapReduceTask<KIn, VIn, KOut, VOut> onKeys(KIn... input) {
      Collections.addAll(keys, input);
      return this;
   }

   /**
    * Specifies Mapper to use for this MapReduceTask
    * <p>
    * Note that {@link Mapper} should not be specified as inner class. Inner classes declared in
    * non-static contexts contain implicit non-transient references to enclosing class instances,
    * serializing such an inner class instance will result in serialization of its associated outer
    * class instance as well.
    * 
    * @param mapper used to execute map phase of MapReduceTask
    * @return this MapReduceTask itself
    */
   public MapReduceTask<KIn, VIn, KOut, VOut> mappedWith(Mapper<KIn, VIn, KOut, VOut> mapper) {
      if (mapper == null)
         throw new IllegalArgumentException("A valid reference of Mapper is needed");
      this.mapper = mapper;
      return this;
   }

   /**
    * Specifies Reducer to use for this MapReduceTask
    * 
    * <p>
    * Note that {@link Reducer} should not be specified as inner class. Inner classes declared in
    * non-static contexts contain implicit non-transient references to enclosing class instances,
    * serializing such an inner class instance will result in serialization of its associated outer
    * class instance as well.
    * 
    * @param reducer used to reduce results of map phase
    * @return this MapReduceTask itself
    */
   public MapReduceTask<KIn, VIn, KOut, VOut> reducedWith(Reducer<KOut, VOut> reducer) {
      if (reducer == null)
         throw new IllegalArgumentException("A valid reference of Reducer is needed");
      this.reducer = reducer;
      return this;
   }
   
   /**
    * Specifies Combiner to use for this MapReduceTask
    * 
    * <p>
    * Note that {@link Reducer} should not be specified as inner class. Inner classes declared in
    * non-static contexts contain implicit non-transient references to enclosing class instances,
    * serializing such an inner class instance will result in serialization of its associated outer
    * class instance as well.
    * 
    * @param combiner used to immediately combine results of map phase before reduce phase is invoked  
    * @return this MapReduceTask itself
    * @since 5.2
    */
   public MapReduceTask<KIn, VIn, KOut, VOut> combinedWith(Reducer<KOut, VOut> combiner) {
      if (combiner == null)
         throw new IllegalArgumentException("A valid reference of Reducer/Combiner is needed");
      this.combiner = combiner;
      return this;
   }

   /**
    * Executes this task across Infinispan cluster nodes.
    * 
    * @return a Map where each key is an output key and value is reduced value for that output key
    */
   public Map<KOut, VOut> execute() throws CacheException {
      if (mapper == null)
         throw new NullPointerException("A valid reference of Mapper is not set " + mapper);

      if (reducer == null)
         throw new NullPointerException("A valid reference of Reducer is not set " + reducer);
      
      
      if(distributeReducePhase()){
         boolean useCompositeKeys = useIntermediateSharedCache();
         String intermediateCacheName = DEFAULT_TMP_CACHE_CONFIGURATION_NAME;
         if (useIntermediatePerTaskCache()) {
            intermediateCacheName = taskId.toString();
         }

         try {
            // init and create tmp caches
            executeTaskInit(intermediateCacheName);

            // map
            Set<KOut> allMapPhasesResponses = executeMapPhase(useCompositeKeys);

            // reduce
            return executeReducePhase(allMapPhasesResponses, useCompositeKeys);
         }
         catch (Exception cause){
            throw new CacheException(cause);
         } finally {
            // cleanup tmp caches across cluster
            if(useIntermediatePerTaskCache()){
               EmbeddedCacheManager cm = cache.getCacheManager();
               cm.getCache(intermediateCacheName).clear();
               cm.removeCache(intermediateCacheName);
            }
         }
      } else {
         try {
            return executeMapPhaseWithLocalReduction();
         } catch (Exception cause){
            throw new CacheException(cause);
         }
      }     
   }
   
   protected boolean distributeReducePhase(){
      return distributeReducePhase;
   }
   
   protected boolean useIntermediateSharedCache() {
      return useIntermediateSharedCache;
   }
   
   protected boolean useIntermediatePerTaskCache() {
      return !useIntermediateSharedCache();
   }


   protected void executeTaskInit(String tmpCacheName) {
      RpcManager rpc = cache.getRpcManager();
      CommandsFactory factory = cache.getComponentRegistry().getComponent(CommandsFactory.class);

      //first create tmp caches on all nodes
      final CreateCacheCommand ccc = factory.buildCreateCacheCommand(tmpCacheName, DEFAULT_TMP_CACHE_CONFIGURATION_NAME, true, rpc.getMembers().size());

      log.debugf("Invoking %s across members %s ", ccc, cache.getRpcManager().getMembers());
      mapReduceManager.getExecutorService().submit(new Callable<Object>() {
         @Override
         public Object call() throws Exception {
            //locally
            ccc.init(cache.getCacheManager());
            try {
               return ccc.perform(null);
            } catch (Throwable e) {
               throw new CacheException("Could not initialize temporary caches for MapReduce task on remote nodes ", e);
            }
         }
      });
      rpc.invokeRemotely(cache.getRpcManager().getMembers(), ccc, rpc.getDefaultRpcOptions(true));
   }

   protected Set<KOut> executeMapPhase(boolean useCompositeKeys) throws InterruptedException,
            ExecutionException {
      RpcManager rpc = cache.getRpcManager();
      MapCombineCommand<KIn, VIn, KOut, VOut> cmd = null;
      Set<KOut> mapPhasesResult = new HashSet<KOut>();
      List<MapTaskPart<Set<KOut>>> futures = new ArrayList<MapTaskPart<Set<KOut>>>();
      if (inputTaskKeysEmpty()) {
         for (Address target : rpc.getMembers()) {
            if (target.equals(rpc.getAddress())) {
               cmd = buildMapCombineCommand(taskId.toString(), clone(mapper), clone(combiner),
                        null, true, useCompositeKeys);
            } else {
               cmd = buildMapCombineCommand(taskId.toString(), mapper, combiner, null, true,
                        useCompositeKeys);
            }
            MapTaskPart<Set<KOut>> part = createTaskMapPart(cmd, target, true);
            part.execute();
            futures.add(part);
         }
      } else {
         Map<Address, List<KIn>> keysToNodes = mapKeysToNodes(keys);
         for (Entry<Address, List<KIn>> e : keysToNodes.entrySet()) {
            Address address = e.getKey();
            List<KIn> keys = e.getValue();
            if (address.equals(rpc.getAddress())) {
               cmd = buildMapCombineCommand(taskId.toString(), clone(mapper), clone(combiner),
                        keys, true, useCompositeKeys);
            } else {
               cmd = buildMapCombineCommand(taskId.toString(), mapper, combiner, keys, true,
                        useCompositeKeys);
            }
            MapTaskPart<Set<KOut>> part = createTaskMapPart(cmd, address, true);
            part.execute();
            futures.add(part);
         }
      }
      try {
         for (MapTaskPart<Set<KOut>> mapTaskPart : futures) {
            mapPhasesResult.addAll(mapTaskPart.get());
         }
      } finally {
         cancellableTasks.clear();
      }
      return mapPhasesResult;
   }

   protected Map<KOut, VOut> executeMapPhaseWithLocalReduction() throws InterruptedException,
            ExecutionException {
      RpcManager rpc = cache.getRpcManager();
      MapCombineCommand<KIn, VIn, KOut, VOut> cmd = null;
      Map<KOut, List<VOut>> mapPhasesResult = new HashMap<KOut, List<VOut>>();
      List<MapTaskPart<Map<KOut, List<VOut>>>> futures = new ArrayList<MapTaskPart<Map<KOut, List<VOut>>>>();
      if (inputTaskKeysEmpty()) {
         for (Address target : rpc.getMembers()) {
            if (target.equals(rpc.getAddress())) {
               cmd = buildMapCombineCommand(taskId.toString(), clone(mapper), clone(combiner),
                        null, false, false);
            } else {
               cmd = buildMapCombineCommand(taskId.toString(), mapper, combiner, null, false, false);
            }
            MapTaskPart<Map<KOut, List<VOut>>> part = createTaskMapPart(cmd, target, false);
            part.execute();
            futures.add(part);
         }
      } else {
         Map<Address, List<KIn>> keysToNodes = mapKeysToNodes(keys);
         for (Entry<Address, List<KIn>> e : keysToNodes.entrySet()) {
            Address address = e.getKey();
            List<KIn> keys = e.getValue();
            if (address.equals(rpc.getAddress())) {
               cmd = buildMapCombineCommand(taskId.toString(), clone(mapper), clone(combiner),
                        keys, false, false);
            } else {
               cmd = buildMapCombineCommand(taskId.toString(), mapper, combiner, keys, false, false);
            }
            MapTaskPart<Map<KOut, List<VOut>>> part = createTaskMapPart(cmd, address, false);
            part.execute();
            futures.add(part);
         }
      }      
      Map<KOut, VOut> reducedResult = new HashMap<KOut, VOut>();
      try {
         for (MapTaskPart<Map<KOut, List<VOut>>> mapTaskPart : futures) {
            // TODO in parallel with futures
            mergeResponse(mapPhasesResult, mapTaskPart.get());
         }
      } finally {
         cancellableTasks.clear();
      }

      // hook into lifecycle
      MapReduceTaskLifecycleService taskLifecycleService = MapReduceTaskLifecycleService
               .getInstance();
      log.tracef("For m/r task %s invoking %s locally", taskId, reducer);
      try {
         taskLifecycleService.onPreExecute(reducer, cache);
         for (Entry<KOut, List<VOut>> e : mapPhasesResult.entrySet()) {
            // TODO in parallel with futures
            reducedResult.put(e.getKey(), reducer.reduce(e.getKey(), e.getValue().iterator()));
         }
      } finally {
         taskLifecycleService.onPostExecute(reducer);
      }
      return reducedResult;
   }

   protected <V> MapTaskPart<V> createTaskMapPart(MapCombineCommand<KIn, VIn, KOut, VOut> cmd,
            Address target, boolean distributedReduce) {
      MapTaskPart<V> mapTaskPart = new MapTaskPart<V>(target, cmd, distributedReduce);
      cancellableTasks.add(mapTaskPart);
      return mapTaskPart;
   }

   protected Map<KOut, VOut> executeReducePhase(Set<KOut> allMapPhasesResponses,
            boolean useCompositeKeys) throws InterruptedException, ExecutionException {
      RpcManager rpc = cache.getRpcManager();
      String destCache = null;
      if (useCompositeKeys) {
         destCache = DEFAULT_TMP_CACHE_CONFIGURATION_NAME;
      } else {
         destCache = taskId.toString();
      }
      Cache<Object, Object> dstCache = cache.getCacheManager().getCache(destCache);
      Map<Address, List<KOut>> keysToNodes = mapKeysToNodes(dstCache.getAdvancedCache()
               .getDistributionManager(), allMapPhasesResponses, useCompositeKeys);
      Map<KOut, VOut> reduceResult = new HashMap<KOut, VOut>();
      List<ReduceTaskPart<Map<KOut, VOut>>> reduceTasks = new ArrayList<ReduceTaskPart<Map<KOut, VOut>>>();
      ReduceCommand<KOut, VOut> reduceCommand = null;
      for (Entry<Address, List<KOut>> e : keysToNodes.entrySet()) {
         Address address = e.getKey();
         List<KOut> keys = e.getValue();
         if (address.equals(rpc.getAddress())) {
            reduceCommand = buildReduceCommand(taskId.toString(), destCache, clone(reducer), keys,
                     useCompositeKeys);
         } else {
            reduceCommand = buildReduceCommand(taskId.toString(), destCache, reducer, keys,
                     useCompositeKeys);
         }
         ReduceTaskPart<Map<KOut, VOut>> part = createReducePart(reduceCommand, address, destCache);
         part.execute();
         reduceTasks.add(part);
      }
      try {
         for (ReduceTaskPart<Map<KOut, VOut>> reduceTaskPart : reduceTasks) {
            reduceResult.putAll(reduceTaskPart.get());
         }
      } finally {
         cancellableTasks.clear();
      }
      return reduceResult;
   }

   protected <V> ReduceTaskPart<V> createReducePart(ReduceCommand<KOut, VOut> cmd, Address target,
            String destCacheName) {
      ReduceTaskPart<V> part = new ReduceTaskPart<V>(target, cmd, destCacheName);
      cancellableTasks.add(part);
      return part;
   }

   private <K, V> void mergeResponse(Map<K, List<V>> result, Map<K, List<V>> m) {
      for (Entry<K, List<V>> entry : m.entrySet()) {
         synchronized (result) {
            List<V> list = result.get(entry.getKey());
            if (list != null) {
               list.addAll(entry.getValue());
            } else {
               list = new ArrayList<V>();
               list.addAll(entry.getValue());
            }
            result.put(entry.getKey(), list);
         }
      }
   }

   private MapCombineCommand<KIn, VIn, KOut, VOut> buildMapCombineCommand(
            String taskId, Mapper<KIn, VIn, KOut, VOut> m, Reducer<KOut, VOut> r,
            Collection<KIn> keys, boolean reducePhaseDistributed, boolean emitCompositeIntermediateKeys){
      ComponentRegistry registry = cache.getComponentRegistry();
      CommandsFactory factory = registry.getComponent(CommandsFactory.class);
      MapCombineCommand<KIn, VIn, KOut, VOut> c = factory.buildMapCombineCommand(taskId, m, r, keys);
      c.setReducePhaseDistributed(reducePhaseDistributed);
      c.setEmitCompositeIntermediateKeys(emitCompositeIntermediateKeys);
      return c;
   }

   private ReduceCommand<KOut, VOut> buildReduceCommand(String taskId,
            String destinationCache, Reducer<KOut, VOut> r, Collection<KOut> keys, boolean emitCompositeIntermediateKeys){
      ComponentRegistry registry = cache.getComponentRegistry();
      CommandsFactory factory = registry.getComponent(CommandsFactory.class);
      ReduceCommand<KOut,VOut> reduceCommand = factory.buildReduceCommand(taskId, destinationCache, r, keys);
      reduceCommand.setEmitCompositeIntermediateKeys(emitCompositeIntermediateKeys);
      return reduceCommand;
   }

   private CancelCommand buildCancelCommand(CancellableTaskPart taskPart){
      ComponentRegistry registry = cache.getComponentRegistry();
      CommandsFactory factory = registry.getComponent(CommandsFactory.class);
      return factory.buildCancelCommandCommand(taskPart.getUUID());
   }

   /**
    * Executes this task across Infinispan cluster nodes asynchronously.
    * 
    * @return a Future wrapping a Map where each key is an output key and value is reduced value for
    *         that output key
    */
   public Future<Map<KOut, VOut>> executeAsynchronously() {
      return new MapReduceTaskFuture<Map<KOut, VOut>>(new Callable<Map<KOut, VOut>>() {

         @Override
         public Map<KOut, VOut> call() throws Exception {
            return execute();
         }
      });
   }

   /**
    * Executes this task across Infinispan cluster but the final result is collated using specified
    * {@link Collator}
    * 
    * @param collator
    *           a Collator to use
    *           
    * @return collated result
    */
   public <R> R execute(Collator<KOut, VOut, R> collator) {
      Map<KOut, VOut> execute = execute();
      return collator.collate(execute);
   }

   /**
    * Executes this task asynchronously across Infinispan cluster; final result is collated using
    * specified {@link Collator} and wrapped by Future
    * 
    * @param collator
    *           a Collator to use
    * 
    * @return collated result
    */
   public <R> Future<R> executeAsynchronously(final Collator<KOut, VOut, R> collator) {
      return new MapReduceTaskFuture<R>(new Callable<R>() {

         @Override
         public R call() throws Exception {
            return execute(collator);
         }
      });
   }

   protected void aggregateReducedResult(Map<KOut, List<VOut>> finalReduced, Map<KOut, VOut> mapReceived) {
      for (Entry<KOut, VOut> entry : mapReceived.entrySet()) {
         List<VOut> l;
         if (!finalReduced.containsKey(entry.getKey())) {
            l = new LinkedList<VOut>();
            finalReduced.put(entry.getKey(), l);
         } else {
            l = finalReduced.get(entry.getKey());
         }
         l.add(entry.getValue());
      }
   }

   protected <T> Map<Address, List<T>> mapKeysToNodes(DistributionManager dm, Collection<T> keysToMap, boolean useIntermediateCompositeKey) {
      return mapReduceManager.mapKeysToNodes(dm, taskId.toString(), keysToMap, useIntermediateCompositeKey);
   }
   
   protected <T> Map<Address, List<T>> mapKeysToNodes(Collection<T> keysToMap, boolean useIntermediateCompositeKey) {
      return mapReduceManager.mapKeysToNodes(cache.getDistributionManager(), taskId.toString(), keysToMap, useIntermediateCompositeKey);
   }
   
   protected <T> Map<Address, List<T>> mapKeysToNodes(Collection<T> keysToMap) {
      return mapKeysToNodes(keysToMap, false);
   }
   
   protected Mapper<KIn, VIn, KOut, VOut> clone(Mapper<KIn, VIn, KOut, VOut> mapper){      
      return Util.cloneWithMarshaller(marshaller, mapper);
   }
   
   protected Reducer<KOut, VOut> clone(Reducer<KOut, VOut> reducer){      
      return Util.cloneWithMarshaller(marshaller, reducer);
   }
   
   private void ensureProperCacheState(AdvancedCache<KIn, VIn> cache) throws NullPointerException,
            IllegalStateException {
      
      if (cache.getRpcManager() == null)
         throw new IllegalStateException("Can not use non-clustered cache for MapReduceTask");

      if (cache.getStatus() != ComponentStatus.RUNNING)
         throw new IllegalStateException("Invalid cache state " + cache.getStatus());

      if (cache.getDistributionManager() == null) {
         throw new IllegalStateException("Cache mode should be DIST, rather than "
                           + cache.getCacheConfiguration().clustering().cacheModeString());
      }
   }

   protected boolean inputTaskKeysEmpty() {
      return keys == null || keys.isEmpty();
   }
   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((taskId == null) ? 0 : taskId.hashCode());
      return result;
   }

   @SuppressWarnings("rawtypes")
   @Override
   public boolean equals(Object obj) {
      if (this == obj) {
         return true;
      }
      if (obj == null) {
         return false;
      }
      if (!(obj instanceof MapReduceTask)) {
         return false;
      }
      MapReduceTask other = (MapReduceTask) obj;
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
      return "MapReduceTask [mapper=" + mapper + ", reducer=" + reducer + ", combiner=" + combiner
               + ", keys=" + keys + ", taskId=" + taskId + "]";
   }

   private class MapReduceTaskFuture<R> extends AbstractInProcessFuture<R> {

      private final Callable<R> call;
      private volatile boolean cancelled = false;
      private volatile boolean done = false;

      public MapReduceTaskFuture(Callable<R> call) {
         super();
         this.call = call;
      }

      @Override
      public R get() throws InterruptedException, ExecutionException {
         if (isCancelled())
            throw new CancellationException("MapReduceTask already cancelled");
         try {
            return call.call();
         } catch (Exception e) {
            throw new ExecutionException(e);
         } finally {
            done = true;
         }
      }

      @Override
      public boolean cancel(boolean mayInterruptIfRunning) {
         if (!isCancelled()) {
            RpcManager rpc = cache.getRpcManager();
            synchronized (cancellableTasks) {
               for (CancellableTaskPart task : cancellableTasks) {
                  boolean sendingToSelf = task.getExecutionTarget().equals(
                           rpc.getTransport().getAddress());
                  CancelCommand cc = buildCancelCommand(task);
                  if (sendingToSelf) {
                     cc.init(cancellationService);
                     try {
                        cc.perform(null);
                     } catch (Throwable e) {
                        log.couldNotExecuteCancellationLocally(e.getLocalizedMessage());
                     }
                  } else {
                     rpc.invokeRemotely(Collections.singletonList(task.getExecutionTarget()), cc, rpc.getDefaultRpcOptions(true));
                  }
                  cancelled = true;
                  done = true;
               }
            }
            return cancelled;
         } else {
            //already cancelled 
            return false;
         }
      }

      @Override
      public boolean isCancelled() {
         return cancelled;
      }

      @Override
      public boolean isDone() {
         return done;
      }
   }

   private abstract class TaskPart<V> implements NotifyingNotifiableFuture<V>, CancellableTaskPart {

      private Future<V> f;
      private final Address executionTarget;

      public TaskPart(Address executionTarget) {
         this.executionTarget = executionTarget;
      }

      @Override
      public Address getExecutionTarget() {
         return executionTarget;
      }

      @Override
      public NotifyingFuture<V> attachListener(FutureListener<V> listener) {
         return this;
      }

      @Override
      public boolean cancel(boolean mayInterruptIfRunning) {
         return false;
      }

      @Override
      public boolean isCancelled() {
         return false;
      }

      @Override
      public boolean isDone() {
         return false;
      }

      @Override
      public V get() throws InterruptedException, ExecutionException {
         return retrieveResult(f.get());
      }

      protected Address getAddress() {
         return cache.getRpcManager().getAddress();
      }

      protected boolean locallyExecuted(){
         return getAddress().equals(getExecutionTarget());
      }

      public abstract void execute();

      @SuppressWarnings("unchecked")
      private V retrieveResult(Object response) throws ExecutionException {
         if (response == null) {
            throw new ExecutionException("Execution returned null value",
                     new NullPointerException());
         }
         if (response instanceof Exception) {
            throw new ExecutionException((Exception) response);
         }

         Map<Address, Response> mapResult = (Map<Address, Response>) response;
         assert mapResult.size() == 1;
         for (Entry<Address, Response> e : mapResult.entrySet()) {
            if (e.getValue() instanceof SuccessfulResponse) {
               return (V) ((SuccessfulResponse) e.getValue()).getResponseValue();
            }
         }
         throw new ExecutionException(new IllegalStateException("Invalid response " + response));
      }

      @Override
      public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException,
               TimeoutException {
         return retrieveResult(f.get(timeout, unit));
      }

      @Override
      public void notifyDone() {
      }

      @Override
      public void setNetworkFuture(Future<V> future) {
         this.f = future;
      }
   }

   private class MapTaskPart<V> extends TaskPart<V> {

      private MapCombineCommand<KIn, VIn, KOut, VOut> mcc;
      private boolean distributedReduce;

      public MapTaskPart(Address executionTarget, MapCombineCommand<KIn, VIn, KOut, VOut> command,
               boolean distributedReduce) {
         super(executionTarget);
         this.mcc = command;
         this.distributedReduce = distributedReduce;
      }

      @Override
      @SuppressWarnings("unchecked")
      public void execute() {
         if (locallyExecuted()) {
            Callable<Map<Address, ? extends Response>> callable;
            if (distributedReduce) {
               callable = new Callable<Map<Address, ? extends Response>>() {

                  @Override
                  public Map<Address, ? extends Response> call() throws Exception {
                     Set<KOut> result = invokeMapCombineLocally();
                     return Collections.singletonMap(getAddress(),
                              SuccessfulResponse.create(result));
                  }
               };
            } else {
               callable = new Callable<Map<Address, ? extends Response>>() {

                  @Override
                  public Map<Address, ? extends Response> call() throws Exception {
                     Map<KOut, List<VOut>> result = invokeMapCombineLocallyForLocalReduction();
                     return Collections.singletonMap(getAddress(),
                              SuccessfulResponse.create(result));
                  }
               };
            }
            FutureTask<V> futureTask = new FutureTask<V>((Callable<V>) callable);
            setNetworkFuture(futureTask);
            mapReduceManager.getExecutorService().submit(futureTask);
         } else {
            RpcManager rpc = cache.getRpcManager();
            try {
               log.debugf("Invoking %s on %s", mcc, getExecutionTarget());
               rpc.invokeRemotelyInFuture(Collections.singleton(getExecutionTarget()), mcc, rpc.getDefaultRpcOptions(true),
                        (NotifyingNotifiableFuture<Object>) this);
               log.debugf("Invoked %s on %s ", mcc, getExecutionTarget());
            } catch (Exception ex) {
               throw new CacheException(
                        "Could not invoke map phase of MapReduceTask on remote node "
                                 + getExecutionTarget(), ex);
            }
         }
      }

      private Map<KOut, List<VOut>> invokeMapCombineLocallyForLocalReduction() throws InterruptedException {
         log.debugf("Invoking %s locally", mcc);
         try {
            cancellationService.register(Thread.currentThread(), mcc.getUUID());
            mcc.init(mapReduceManager);
            return mapReduceManager.mapAndCombineForLocalReduction(mcc);
         } finally {
            cancellationService.unregister(mcc.getUUID());
            log.debugf("Invoked %s locally", mcc);
         }
      }

      private Set<KOut> invokeMapCombineLocally() throws InterruptedException {
         log.debugf("Invoking %s locally", mcc);
         try {
            cancellationService.register(Thread.currentThread(), mcc.getUUID());
            mcc.init(mapReduceManager);
            return mapReduceManager.mapAndCombineForDistributedReduction(mcc);
         } finally {
            cancellationService.unregister(mcc.getUUID());
            log.debugf("Invoked %s locally", mcc);
         }
      }

      @Override
      public UUID getUUID() {
         return mcc.getUUID();
      }
   }

   private class ReduceTaskPart<V> extends TaskPart<V> {

      private ReduceCommand<KOut, VOut> rc;
      private String cacheName;

      public ReduceTaskPart(Address executionTarget, ReduceCommand<KOut, VOut> command,
               String destinationCacheName) {
         super(executionTarget);
         this.rc = command;
         this.cacheName = destinationCacheName;
      }

      @Override
      @SuppressWarnings("unchecked")
      public void execute() {
         if (locallyExecuted()) {
            Callable<Map<Address, ? extends Response>> callable = new Callable<Map<Address, ? extends Response>>() {

               @Override
               public Map<Address, ? extends Response> call() throws Exception {
                  Cache<Object, Object> dstCache = cache.getCacheManager().getCache(cacheName);
                  Map<KOut, VOut> result = invokeReduceLocally(dstCache);
                  return Collections.singletonMap(getAddress(), SuccessfulResponse.create(result));
               }
            };
            FutureTask<V> futureTask = new FutureTask<V>((Callable<V>) callable);
            setNetworkFuture(futureTask);
            mapReduceManager.getExecutorService().submit(futureTask);
         } else {
            RpcManager rpc = cache.getRpcManager();
            try {
               log.debugf("Invoking %s on %s", rc, getExecutionTarget());
               rpc.invokeRemotelyInFuture(Collections.singleton(getExecutionTarget()), rc, rpc.getDefaultRpcOptions(true),
                        (NotifyingNotifiableFuture<Object>) this);
               log.debugf("Invoked %s on %s ", rc, getExecutionTarget());
            } catch (Exception ex) {
               throw new CacheException(
                        "Could not invoke map phase of MapReduceTask on remote node "
                                 + getExecutionTarget(), ex);
            }
         }
      }

      private Map<KOut, VOut> invokeReduceLocally(Cache<Object, Object> dstCache) {
         rc.init(mapReduceManager);
         Map<KOut, VOut> localReduceResult = null;
         try {
            log.debugf("Invoking %s locally ", rc);
            localReduceResult = mapReduceManager.reduce(rc);
            log.debugf("Invoked %s locally", rc);
         } catch (Throwable e1) {
            throw new CacheException("Could not invoke MapReduce task locally ", e1);
         }
         return localReduceResult;
      }

      @Override
      public UUID getUUID() {
         return rc.getUUID();
      }
   }

   private interface CancellableTaskPart {
      UUID getUUID();
      Address getExecutionTarget();
   }
}
