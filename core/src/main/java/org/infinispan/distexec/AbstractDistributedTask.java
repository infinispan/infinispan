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
package org.infinispan.distexec;

import java.util.Map;

import org.infinispan.Cache;
import org.infinispan.remoting.transport.Address;

/**
 * AbstractDistributedTask class provides settings for various distributed task characteristics such
 * as fail-over and cancellation policies etc etc
 * 
 * 
 * @author Manik Surtani
 * @author Vladimir Blagojevic
 * 
 * @since 5.0
 */
public class AbstractDistributedTask<K, V, T, R> {

   protected final Cache<K,V> cache;

   public AbstractDistributedTask(Cache<K,V> cache) {
      super();
      this.cache = cache;
   }

   /**
    * Invoked by execution environment before DistributedTask map phase is invoked.
    * <tt>getCacheName</tt> specifies cache used as input data for DistributedTask.
    * 
    * 
    * @return collection of keys used as an input
    */
   public String getCacheName() {
      return cache.getName();
   }

   /**
    * Maps DistributedCallables to Infinispan nodes. DistributedCallables are going to be migrated
    * for execution to nodes according to a returned execution map.
    * 
    * 
    * @param input
    *           input keys for this task
    * 
    * @return map of DistributedCallables to be executed in Infinispan cloud where each
    *         DistributedCallable is mapped to a particular node for execution
    * @throws Exception
    */
   public Map<DistributedCallable<K, V, T>, Address> executionMap(K... input) throws Exception {

      // do the default or whatever mapping of callables using factory to create DistributedCallable
      // TODO map callable to nodes according to CH
      return null;
   }

   /**
    * FIXME Comment this
    * 
    */
   public void setFailOverPolicy() {
   }

   /**
    * FIXME Comment this
    * 
    */
   public void setExecutionNodeSplittingPolicy() {
   }

   /**
    * FIXME Comment this
    * 
    */
   public void setCancelationPolicy() {
   }

   /*
    * 
    * And other task level characteristics....
    */

}
