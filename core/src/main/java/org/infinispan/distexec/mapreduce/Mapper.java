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

/**
 * Implementation of a Mapper class is a component of a MapReduceTask invoked once for each input
 * entry K,V. Every Mapper instance migrated to an Infinispan node, given a cache entry K,V input
 * pair transforms that input pair into a result T. Intermediate result T is further reduced using a
 * Reducer.
 * 
 * 
 * @see Reducer
 * @see MapReduceTask
 * 
 * @author Manik Surtani
 * @author Vladimir Blagojevic
 * 
 * @since 5.0
 */
public interface Mapper<K, V, T> {

   /**
    * Invoked once for each input cache entry K,V transforms that input into a result T.
    * 
    * @param key
    *           the kay
    * @param value
    *           the value
    * @return result T
    */
   T map(K key, V value);

}
