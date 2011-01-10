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
 * Reduces a list of results T from map phase of MapReduceTask. Infinispan distributed execution
 * environment creates one instance of Reducer per execution node.
 * 
 * @author Manik Surtani
 * @author Vladimir Blagojevic
 * 
 * @since 5.0
 */
public interface Reducer<T, R> {

   /**
    * Reduces a result T from map phase and return R.
    * <p>
    * 
    * Assume that on Infinispan node N, an instance of Mapper was mapped and invoked on k many
    * key/value pairs. Each T(i) in the list of all T's returned from map phase executed on
    * Infinispan node N is passed to reducer along with previsouly computed R(i-1). Finally the last
    * invocation of reducer on T(k), R is returned to a distributed task that originated map/reduce
    * request.
    * 
    * @param mapResult
    *           result T of map phase
    * @param previouslyReduced
    *           previously accumulated reduced result
    * @return result R
    * 
    */
   R reduce(T mapResult, R previouslyReduced);

}
