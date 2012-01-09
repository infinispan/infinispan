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
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.read.MapReduceCommand;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.marshall.Marshaller;
import org.infinispan.marshall.StreamingMarshaller;
import org.infinispan.remoting.responses.ExceptionResponse;
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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
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

   private Mapper<KIn, VIn, KOut, VOut> mapper;
   private Reducer<KOut, VOut> reducer;

   private final Collection<KIn> keys;
   private final AdvancedCache<KIn, VIn> cache;
   protected final Marshaller marshaller;

   /**
    * Create a new MapReduceTask given a master cache node. All distributed task executions will be
    * initiated from this cache node.
    * 
    * @param masterCacheNode
    *           cache node initiating map reduce task
    */
   public MapReduceTask(Cache<KIn, VIn> masterCacheNode) {
      if (masterCacheNode == null)
         throw new NullPointerException("Can not use " + masterCacheNode
                  + " cache for MapReduceTask");

      ensureProperCacheState(masterCacheNode.getAdvancedCache());      
      this.cache = masterCacheNode.getAdvancedCache();
      this.keys = new LinkedList<KIn>();
      this.marshaller = cache.getComponentRegistry().getComponent(StreamingMarshaller.class, CACHE_MARSHALLER);
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
    * @param mapper
    * @return
    */
   public MapReduceTask<KIn, VIn, KOut, VOut> mappedWith(Mapper<KIn, VIn, KOut, VOut> mapper) {
      if (mapper == null)
         throw new NullPointerException("A valid reference of Mapper is needed " + mapper);
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
    * @param reducer
    * @return
    */
   public MapReduceTask<KIn, VIn, KOut, VOut> reducedWith(Reducer<KOut, VOut> reducer) {
      if (reducer == null)
         throw new NullPointerException("A valid reference of Mapper is needed " + reducer);
      this.reducer = reducer;
      return this;
   }

   /**
    * Executes this task across Infinispan cluster nodes.
    * 
    * @return a Map where each key is an output key and value is reduced value for that output key
    */
   @SuppressWarnings("unchecked")
   public Map<KOut, VOut> execute() throws CacheException {
      if (mapper == null)
         throw new NullPointerException("A valid reference of Mapper is not set " + mapper);
      
      if (reducer == null)
         throw new NullPointerException("A valid reference of Reducer is not set " + reducer);
            
      ComponentRegistry registry = cache.getComponentRegistry();
      RpcManager rpc = cache.getRpcManager();
      InvocationContextContainer icc = cache.getInvocationContextContainer();
      DistributionManager dm = cache.getDistributionManager();
      InterceptorChain invoker = registry.getComponent(InterceptorChain.class);
      CommandsFactory factory = registry.getComponent(CommandsFactory.class);
      
      MapReduceCommand cmd = null;
      MapReduceCommand selfCmd = null;
      Map<Address, Response> results = new HashMap<Address, Response>();
      if (inputTaskKeysEmpty()) {
         cmd = factory.buildMapReduceCommand(mapper, reducer, rpc.getAddress(), keys);
         selfCmd = cmd;
         try {
            log.debugf("Invoking %s across entire cluster ", cmd);
            Map<Address, Response> map = rpc.invokeRemotely(null, cmd, true, false);
            log.debugf("Invoked %s across entire cluster, results are %s", cmd, map);
            results.putAll(map);
         } catch (Throwable e) {
            throw new CacheException("Could not invoke MapReduce task on remote nodes ", e);
         }
      } else {
         Map<Address, List<KIn>> keysToNodes = mapKeysToNodes();
         log.debugf("Keys to nodes mapping is " + keysToNodes);
         List<MapReduceFuture> futures = new ArrayList<MapReduceFuture>();
         for (Entry<Address, List<KIn>> e : keysToNodes.entrySet()) {
            Address address = e.getKey();
            List<KIn> keys = e.getValue();
            if (address.equals(rpc.getAddress())) {
               selfCmd = factory.buildMapReduceCommand(clone(mapper), clone(reducer), rpc.getAddress(), keys);
            } else {
               cmd = factory.buildMapReduceCommand(mapper, reducer, rpc.getAddress(), keys);
               try {
                  log.debugf("Invoking %s on %s", cmd, address);
                  MapReduceFuture future = new MapReduceFuture();
                  futures.add(future);
                  rpc.invokeRemotelyInFuture(Collections.singleton(address), cmd, future);                  
                  log.debugf("Invoked %s on %s ", cmd, address);
               } catch (Exception ex) {
                  throw new CacheException("Could not invoke MapReduceTask on remote node " + address, ex);
               }
            }
         }
         for (MapReduceFuture future : futures) {
            Map<Address, Response> result;
            try {
               result = (Map<Address, Response>) future.get();               
               results.putAll(result);
               log.debugf("Received result from future %s", result);
            } catch (Exception e1) {
               throw new CacheException("Could not retrieve MapReduceTask result from remote node", e1);
            }            
         }
      }
      boolean selfInvoke = selfCmd != null;
      Object localCommandResult = null;
      if (selfInvoke) {
         log.debugf("Invoking %s locally", cmd);
         selfCmd.init(factory, invoker, icc, dm, rpc.getAddress());
         try {
            localCommandResult = selfCmd.perform(null);
            log.debugf("Invoked %s locally", cmd);
         } catch (Throwable e1) {
            throw new CacheException("Could not invoke MapReduce task locally ", e1);
         }
      }

      // we have results from all nodes now, group intermediate keys for final reduction
      Map<KOut, List<VOut>> reduceMap = new HashMap<KOut, List<VOut>>();
      for (Entry<Address, Response> e : results.entrySet()) {
         Response rsp = e.getValue();
         if (rsp.isSuccessful() && rsp.isValid()) {            
            Map<KOut, VOut> reducedResponse = (Map<KOut, VOut>) ((SuccessfulResponse) rsp).getResponseValue();
            groupKeys(reduceMap, reducedResponse);
         } else if (rsp instanceof ExceptionResponse) {
            throw new CacheException("MapReduce task on remote node " + e.getKey()
                     + " threw Exception", ((ExceptionResponse) rsp).getException());
         } else {
            throw new CacheException("MapReduce task on remote node " + e.getKey() + " failed ");
         }
      }

      if (selfInvoke) {
         groupKeys(reduceMap, (Map<KOut, VOut>) localCommandResult);
      }

      // final reduce
      //TODO parallelize into Executor
      Map<KOut, VOut> result = new HashMap<KOut, VOut>();
      for (Entry<KOut, List<VOut>> entry : reduceMap.entrySet()) {
         VOut reduced = reducer.reduce(entry.getKey(), (entry.getValue()).iterator());
         result.put(entry.getKey(), reduced);
      }
      return result;
   }

   /**
    * Executes this task across Infinispan cluster nodes asynchronously.
    * 
    * @return a Future wrapping a Map where each key is an output key and value is reduced value for
    *         that output key
    */
   public Future<Map<KOut, VOut>> executeAsynchronously() {
      final Callable<Map<KOut, VOut>> call = new Callable<Map<KOut, VOut>>() {
         
         @Override
         public Map<KOut, VOut> call() throws Exception {
            return execute();
         }
      };
      return new AbstractInProcessFuture<Map<KOut, VOut>>() {

         @Override
         public Map<KOut, VOut> get() throws InterruptedException, ExecutionException {
            try {
               return call.call();
            } catch (Exception e) {
               throw new ExecutionException(e);
            }
         }
      };
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
      final Callable<R> call = new Callable<R>() {

         @Override
         public R call() throws Exception {
            return execute(collator);
         }
      };
      return new AbstractInProcessFuture<R>() {

         @Override
         public R get() throws InterruptedException, ExecutionException {
            try {
               return call.call();
            } catch (Exception e) {
               throw new ExecutionException(e);
            }
         }
      };
   }

   protected void groupKeys(Map<KOut, List<VOut>> finalReduced, Map<KOut, VOut> mapReceived) {
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

   protected Map<Address, List<KIn>> mapKeysToNodes() {
      DistributionManager dm = cache.getDistributionManager();
      Map<Address, List<KIn>> addressToKey = new HashMap<Address, List<KIn>>();
      for (KIn key : keys) {
         List<Address> nodesForKey = dm.locate(key);
         Address ownerOfKey = nodesForKey.get(0);
         List<KIn> keysAtNode = addressToKey.get(ownerOfKey);
         if (keysAtNode == null) {
            keysAtNode = new ArrayList<KIn>();
            addressToKey.put(ownerOfKey, keysAtNode);
         }
         keysAtNode.add(key);
      }
      return addressToKey;
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
                           + cache.getConfiguration().getCacheModeString());
      }
   }

   private boolean inputTaskKeysEmpty() {
      return keys == null || keys.isEmpty();
   }
   
   private static class MapReduceFuture implements NotifyingNotifiableFuture<Object>{

      private Future<Object> futureResult;

      @Override
      public NotifyingFuture<Object> attachListener(
               FutureListener<Object> listener) {
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
      public Object get() throws InterruptedException,
               ExecutionException {
         return futureResult.get();
      }

      @Override
      public Object get(long timeout, TimeUnit unit)
               throws InterruptedException, ExecutionException, TimeoutException {
         return futureResult.get(timeout, unit);
      }

      @Override
      public void notifyDone() {         
      }

      @Override
      public void setNetworkFuture(Future<Object> future) {
         this.futureResult = future;
      }
   }
}
