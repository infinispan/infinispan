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

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * An Executor that provides methods to submit tasks for an execution on a cluster Infinispan nodes.
 * <p>
 * 
 * Every DistributedExecutorService is bound to one particular cache and the tasks submitted will
 * have access to key/value pairs on that cache if the task submitted is an instance of
 * <code>DistributedCallable<code>
 * <p>
 * 
 * DistributedExecutorService will use default distributed execution policies which can be tuned for each 
 * DistributedExecutorService instance.
 * 
 * 
 * 
 * 
 * @see DistributedCallable
 * 
 * @author Manik Surtani
 * @author Vladimir Blagojevic
 * 
 * @since 5.0
 */
public interface DistributedExecutorService extends ExecutorService {

   /**
    * Submits given Callable task for an execution on a single Infinispan node
    * 
    * @param <T>
    * @param <K>
    * @param task
    * @param input
    * @return
    */
   public <T, K> Future<T> submit(Callable<T> task, K... input);

   /**
    * Submits the given Callable task for an execution on all available Infinispan nodes
    * 
    * @param <T>
    * @param task
    * @return
    */
   public <T> List<Future<T>> submitEverywhere(Callable<T> task);

   /**
    * Submits the given Callable task for an execution on all available Infinispan nodes using input
    * keys specified by K input
    * 
    * @param <T>
    * @param <K>
    * @param task
    * @param input
    * @return
    */
   public <T, K> List<Future<T>> submitEverywhere(Callable<T> task, K... input);
}
