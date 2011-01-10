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

import java.util.concurrent.Future;

import org.infinispan.Cache;
import org.infinispan.distexec.AbstractDistributedTask;

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

   public MapReduceTask(Cache<K,V> cache) {
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
      // TODO
      return this;
   }

   /**
    * Specifies mapper to use for this MapReduceTask
    * 
    * @param mapper
    * @return
    */
   public MapReduceTask<K, V, T, R> mappedWith(Mapper<K, V, T> mapper) {
      // TODO
      return this;
   }

   /**
    * Specifies reducer to use for this MapReduceTask
    * 
    * @param reducer
    * @return
    */
   public MapReduceTask<K, V, T, R> reducedWith(Reducer<R, T> reducer) {
      // TODO
      return this;
   }

   /**
    * Specifies collator to use for this MapReduceTask and returns a result of this task's
    * computation
    * 
    * @param mapper
    * @return
    */
   public R collate(Collator<R> mapper) {
      // TODO
      return null;
   }

   /**
    * Specifies collator to use for this MapReduceTask and returns a result of this task's
    * computation asynchronously
    * 
    * @param mapper
    * @return
    */
   public Future<R> collateAsynchronously(Collator<R> mapper) {
      // TODO
      return null;
   }
}
