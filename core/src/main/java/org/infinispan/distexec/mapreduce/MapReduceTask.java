/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat Middleware LLC, and individual contributors
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
package org.infinispan.distexec.mapreduce;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.infinispan.Cache;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.read.MapReduceCommand;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.distexec.AbstractDistributedTask;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.concurrent.AbstractInProcessFuture;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * MapReduceTask is a distributed task which allows a large scale computation to be transparently
 * parallelized across Infinispan cluster nodes.
 * <p>
 * 
 * Users of MapReduceTask should provide name of the cache whose data is used as input for this
 * task. Infinispan execution environment will instantiate and migrate instances of provided mappers
 * and reducers seamlessly across Infinispan nodes. 
 * <p>
 * 
 * Unless otherwise specified using <code>onKeys</code> input keys filter all available key value
 * pairs of a specified cache will be used as input data for this task
 * 
 * In a nutshell, map reduce task is executed in following fashion:
 * 
 * <pre>
 * On each Infinispan node:
 *
 * {@code 
 * mapped = list() 
 * for entry in cache.entries: 
 *    t = mapper.map(entry.key, entry.value)
 *    mapped.add(t)
 * 
 * r = null 
 * for t in mapped: 
 *    r = reducer.reduce(t, r)
 * return r to Infinispan node that invoked the task
 * 
 * On Infinispan node invoking this task: 
 * reduced_results = invoke map reduce task on all nodes, retrieve map{address:result} 
 * for r in reduced_results.entries: 
 *    remote_address = r.key 
 *    remote_reduced_result = r.value
 *    collator.add(remote_address, remote_reduced_result)
 *
 * return collator.collate()
 * }</pre>
 * 
 * 
 * @author Manik Surtani
 * @author Vladimir Blagojevic
 * 
 * @since 5.0
 */
public class MapReduceTask<K, V, T, R> extends AbstractDistributedTask<K, V, T, R> {

   private static final Log log = LogFactory.getLog(MapReduceTask.class);
   
   private Mapper<K,V,T> mapper;
   private Reducer<R,T> reducer;
   
   private Collection<K> keys;

   public MapReduceTask(Cache<K, V> cache) {
      super(cache);
   }

   /**
    * Rather than use all available keys as input <code>onKeys</code> allows users to specify a
    * subset of keys as input to this task
    * 
    * @param input
    *           input keys for this task
    * @return this task
    */
   public MapReduceTask<K, V, T, R> onKeys(K... input) {
      for (K key : input) {
         keys.add(key);
      }
      return this;
   }

   /**
    * Specifies mapper to use for this MapReduceTask
    * 
    * @param mapper
    * @return
    */
   public MapReduceTask<K, V, T, R> mappedWith(Mapper<K, V, T> mapper) {
      this.mapper = mapper;
      return this;
   }

   /**
    * Specifies reducer to use for this MapReduceTask
    * 
    * @param reducer
    * @return
    */
   public MapReduceTask<K, V, T, R> reducedWith(Reducer<R, T> reducer) {
      this.reducer = reducer;
      return this;
   }

   /**
    * Specifies collator to use for this MapReduceTask and returns a result of this task's
    * computation
    * 
    * @param mapper
    * @return
    */
   public R collate(Collator<R> collator) throws Exception {
      ComponentRegistry registry = cache.getAdvancedCache().getComponentRegistry();
      RpcManager rpc = cache.getAdvancedCache().getRpcManager();
      InvocationContextContainer icc = cache.getAdvancedCache().getInvocationContextContainer();
      DistributionManager dm = cache.getAdvancedCache().getDistributionManager();
      InterceptorChain invoker = registry.getComponent(InterceptorChain.class);
      CommandsFactory factory = registry.getComponent(CommandsFactory.class);

      Map<Address, Response> results = new HashMap<Address, Response>();
      MapReduceCommand cmd = null;
      MapReduceCommand selfInvokeCmd = null;
      if (inputTaskKeysEmpty()) {           
         cmd = factory.buildMapReduceCommand(mapper, reducer, rpc.getAddress(), keys);         
         selfInvokeCmd = cmd;
         try {
            log.debug("Invoking %s across entire cluster ", cmd);
            Map<Address, Response> map = rpc.invokeRemotely(null, cmd, ResponseMode.SYNCHRONOUS, 10000);
            log.debug("Invoked %s across entire cluster, results are %s", cmd, map);
            results.putAll(map);
         } catch (Throwable e) {
            throw new Exception("Could not invoke MapReduce task on remote nodes ", e);
         }
      } else {
         Map<Address, List<K>> keysToNodes = mapKeysToNodes();
         for (Entry<Address, List<K>> e : keysToNodes.entrySet()) {
            if (e.getKey().equals(rpc.getAddress())) {
               selfInvokeCmd = factory.buildMapReduceCommand(mapper, reducer, rpc.getAddress(), e.getValue());
            } else {               
               cmd = factory.buildMapReduceCommand(mapper, reducer, rpc.getAddress(), e.getValue());
               try {
                  log.debug("Invoking %s on %s", cmd, e.getKey());
                  Map<Address, Response> r = rpc.invokeRemotely(Collections.singleton(e.getKey()),
                           cmd, ResponseMode.SYNCHRONOUS, 10000);
                  log.debug("Invoked %s on %s and got result %s", cmd, e.getKey(), r);
                  results.putAll(r);
               } catch (Exception ex) {
                  throw new Exception("Could not invoke MapReduce task on remote node "
                           + e.getKey(), ex);
               }
            }
         }
      }
      boolean selfInvoke = selfInvokeCmd != null;
      Object localCommandResult = null;
      if (selfInvoke) {
         selfInvokeCmd.init(factory, invoker, icc, dm, rpc.getAddress());
         try {
            localCommandResult = selfInvokeCmd.perform(null);
         } catch (Throwable e1) {
            throw new Exception("Could not invoke MapReduce task locally ", e1);
         }
      }

      for (Entry<Address, Response> e : results.entrySet()) {
         Response rsp = e.getValue();
         if (rsp.isSuccessful() && rsp.isValid()) {
            collator.reducedResultReceived(e.getKey(), (R) ((SuccessfulResponse) rsp).getResponseValue());
         } else if (rsp instanceof ExceptionResponse) {
            throw new Exception("Invocation of MapReduce task on remote node " + e.getKey()
                     + " threw Exception", ((ExceptionResponse) rsp).getException());
         } else {
            throw new Exception("Invocation of MapReduce task on remote node " + e.getKey() + " failed ");
         }
      }
      if (selfInvoke) {
         collator.reducedResultReceived(rpc.getAddress(), (R) localCommandResult);
      }
      return collator.collate();
   }

   /**
    * Specifies collator to use for this MapReduceTask and returns a result of this task's
    * computation asynchronously
    * 
    * @param mapper
    * @return
    */
   public Future<R> collateAsynchronously(final Collator<R> collator) {
      final Callable<R> call = new Callable<R>() {

         @Override
         public R call() throws Exception {
            return collate(collator);
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

   private Map<Address, List<K>> mapKeysToNodes() {
      DistributionManager dm = cache.getAdvancedCache().getDistributionManager();
      Map<Address, List<K>> addressToKey = new HashMap<Address, List<K>>();
      for (K key : keys) {
         List<Address> nodesForKey = dm.locate(key);
         Address ownerOfKey = nodesForKey.get(0);
         List<K> keysAtNode = addressToKey.get(ownerOfKey);
         if (keysAtNode == null) {
            keysAtNode = new ArrayList<K>();
            addressToKey.put(ownerOfKey, keysAtNode);
         }
         keysAtNode.add(key);
      }
      return addressToKey;
   }

   private boolean inputTaskKeysEmpty() {
      return keys == null || keys.isEmpty();
   }
}
