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
package org.infinispan.distexec;

import java.io.Externalizable;
import java.io.NotSerializableException;
import java.io.Serializable;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * An ExecutorService that provides methods to submit tasks for execution on a cluster of Infinispan
 * nodes.
 * <p>
 * 
 * Every DistributedExecutorService is bound to one particular cache. Tasks submitted will have
 * access to key/value pairs from that particular cache if and only if the task submitted is an
 * instance of {@link DistributedCallable}. Also note that there is nothing preventing a user from
 * submitting a familiar {@link Runnable} or {@link Callable} just like to any other
 * {@link ExecutorService}. However, DistributedExecutorService, as it name implies, will likely
 * migrate submitted Callable or Runnable to another JVM in Infinispan cluster, execute it and
 * return a result to task invoker.
 * <p>
 * 
 * 
 * Note that due to potential task migration to other nodes every {@link Callable},
 * {@link Runnable} and/or {@link DistributedCallable} submitted must be either {@link Serializable}
 * or {@link Externalizable}. Also the value returned from a callable must be {@link Serializable}
 * or {@link Externalizable}. Unfortunately if the value returned is not serializable then a
 * {@link NotSerializableException} will be thrown.
 * 
 * @see DefaultExecutorService
 * @see DistributedCallable
 * 
 * @author Manik Surtani
 * @author Vladimir Blagojevic
 * 
 * @since 5.0
 */
public interface DistributedExecutorService extends ExecutorService {

   /**
    * Submits given Callable task for an execution on a single Infinispan node.
    * <p>
    * 
    * Execution environment will chose an arbitrary node N hosting some or all of the keys specified
    * as input. If all keys are not available locally at node N they will be retrieved from the
    * cluster.
    * 
    * @param task a task to execute across Infinispan cluster
    * @param input input keys for this task, effective if and only if task is instance of {@link DistributedCallable} 
    * @return a Future representing pending completion of the task
    */
   public <T, K> Future<T> submit(Callable<T> task, K... input);

   /**
    * Submits the given Callable task for an execution on all available Infinispan nodes.
    * 
    * @param task a task to execute across Infinispan cluster
    * @return a list of Futures, one future per Infinispan cluster node where task was executed
    */
   public <T> List<Future<T>> submitEverywhere(Callable<T> task);

   /**
    * Submits the given Callable task for an execution on all available Infinispan nodes using input
    * keys specified by K input.
    * <p>
    * 
    * Execution environment will chose all nodes in Infinispan cluster where input keys are local,
    * migrate given Callable instance to those nodes, execute it and return result as a list of
    * Futures
    * 
    * @param task a task to execute across Infinispan cluster
    * @param input input keys for this task, effective if and only if task is instance of {@link DistributedCallable} 
    * @return a list of Futures, one future per Infinispan cluster node where task was executed
    */
   public <T, K > List<Future<T>> submitEverywhere(Callable<T> task, K... input);
}
