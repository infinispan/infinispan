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

import static org.infinispan.factories.KnownComponentNames.CACHE_MARSHALLER;

import java.io.Externalizable;
import java.io.NotSerializableException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.read.DistributedExecuteCommand;
import org.infinispan.distexec.spi.DistributedTaskLifecycleService;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.marshall.Marshaller;
import org.infinispan.marshall.StreamingMarshaller;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.Util;
import org.infinispan.util.concurrent.FutureListener;
import org.infinispan.util.concurrent.NotifyingFuture;
import org.infinispan.util.concurrent.NotifyingNotifiableFuture;
import org.infinispan.util.concurrent.WithinThreadExecutor;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Infinispan's implementation of an {@link ExecutorService} and {@link DistributedExecutorService}.
 * This ExecutorService provides methods to submit tasks for an execution on a cluster of Infinispan
 * nodes.
 * <p>
 *
 *
 * Note that due to potential task migration to another nodes every {@link Callable},
 * {@link Runnable} and/or {@link DistributedCallable} submitted must be either {@link Serializable}
 * or {@link Externalizable}. Also the value returned from a callable must be {@link Serializable}
 * or {@link Externalizable}. Unfortunately if the value returned is not serializable then a
 * {@link NotSerializableException} will be thrown.
 *
 * @author Vladimir Blagojevic
 * @since 5.0
 *
 */
public class DefaultExecutorService extends AbstractExecutorService implements DistributedExecutorService {

   private static final Log log = LogFactory.getLog(DefaultExecutorService.class);
   protected final AtomicBoolean isShutdown = new AtomicBoolean(false);
   protected final AdvancedCache cache;
   protected final RpcManager rpc;
   protected final InterceptorChain invoker;
   protected final CommandsFactory factory;
   protected final Marshaller marshaller;
   protected final ExecutorService localExecutorService;

   /**
    * Creates a new DefaultExecutorService given a master cache node for local task execution. All
    * distributed task executions will be initiated from this Infinispan cache node
    * 
    * @param masterCacheNode
    *           Cache node initiating distributed task
    */
   public DefaultExecutorService(Cache masterCacheNode) {
      this(masterCacheNode, new WithinThreadExecutor());
   }

   /**
    * Creates a new DefaultExecutorService given a master cache node and an ExecutorService for
    * parallel execution of task ran on this node. All distributed task executions will be initiated
    * from this Infinispan cache node.
    * 
    * @param masterCacheNode
    *           Cache node initiating distributed task
    * @param localExecutorService
    *           ExecutorService to run local tasks
    */
   public DefaultExecutorService(Cache<?, ?> masterCacheNode, ExecutorService localExecutorService){
      super();
      if (masterCacheNode == null)
         throw new IllegalArgumentException("Can not use null cache for DefaultExecutorService");
      else if (localExecutorService == null)
         throw new IllegalArgumentException("Can not use null instance of ExecutorService");
      else if (localExecutorService.isShutdown())
         throw new IllegalArgumentException("Can not use an instance of ExecutorService which is shutdown");

      ensureProperCacheState(masterCacheNode.getAdvancedCache());

      this.cache = masterCacheNode.getAdvancedCache();
      ComponentRegistry registry = cache.getComponentRegistry();

      this.rpc = cache.getRpcManager();
      this.invoker = registry.getComponent(InterceptorChain.class);
      this.factory = registry.getComponent(CommandsFactory.class);
      this.marshaller = registry.getComponent(StreamingMarshaller.class, CACHE_MARSHALLER);
      this.localExecutorService = localExecutorService; 
   }
   
   @Override
   public <T> DistributedTaskBuilder<T> getDistributedTaskBuilder() {
      long to = cache.getCacheConfiguration().clustering().sync().replTimeout();
      DistributedTaskBuilder<T> dtb = new DefaultDistributedTaskBuilder<T>(this, to);
      return dtb;
   }

   @Override
   public <T> NotifyingFuture<T> submit(Runnable task, T result) {
      return (NotifyingFuture<T>) super.submit(task, result);
   }

   @Override
   public <T> NotifyingFuture<T> submit(Callable<T> task) {
      return (NotifyingFuture<T>) super.submit(task);
   }

   @Override
   public void shutdown() {
      realShutdown(false);
   }
   
   protected List<Address> executionCandidates() {
      return rpc.getTransport().getMembers();
   }
   
   private Address getAddress(){
      return rpc.getAddress();
   }

   private List<Runnable> realShutdown(boolean interrupt) {
      isShutdown.set(true);
      // TODO cancel all tasks
      localExecutorService.shutdownNow();
      return Collections.emptyList();
   }

   @Override
   public List<Runnable> shutdownNow() {
      return realShutdown(true);
   }

   @Override
   public boolean isShutdown() {
      return isShutdown.get();
   }

   @Override
   public boolean isTerminated() {
      if (isShutdown.get()) {
         // TODO account for all tasks
         return true;
      }
      return false;
   }

   @Override
   public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
      //long nanoTimeWait = unit.toNanos(timeout);
      // TODO wait for all tasks to finish
      return true;
   }

   @Override
   public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException,
            ExecutionException {
      try {
         return doInvokeAny(tasks, false, 0);
      } catch (TimeoutException cannotHappen) {
         assert false;
         return null;
      }
   }

   @Override
   public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
      return doInvokeAny(tasks, true, unit.toNanos(timeout));
   }

   /**
    * the main mechanics of invokeAny. This was essentially copied from
    * {@link AbstractExecutorService} doInvokeAny except that we replaced the
    * {@link ExecutorCompletionService} with our {@link DistributedExecutionCompletionService}.
    */
   private <T> T doInvokeAny(Collection<? extends Callable<T>> tasks, boolean timed, long nanos)
            throws InterruptedException, ExecutionException, TimeoutException {
      if (tasks == null)
         throw new NullPointerException();
      int ntasks = tasks.size();
      if (ntasks == 0)
         throw new IllegalArgumentException();
      List<Future<T>> futures = new ArrayList<Future<T>>(ntasks);
      CompletionService<T> ecs = new DistributedExecutionCompletionService<T>(this);

      // For efficiency, especially in executors with limited
      // parallelism, check to see if previously submitted tasks are
      // done before submitting more of them. This interleaving
      // plus the exception mechanics account for messiness of main
      // loop.

      try {
         // Record exceptions so that if we fail to obtain any
         // result, we can throw the last exception we got.
         ExecutionException ee = null;
         long lastTime = (timed) ? System.nanoTime() : 0;
         Iterator<? extends Callable<T>> it = tasks.iterator();

         // Start one task for sure; the rest incrementally
         futures.add(ecs.submit(it.next()));
         --ntasks;
         int active = 1;

         for (;;) {
            Future<T> f = ecs.poll();
            if (f == null) {
               if (ntasks > 0) {
                  --ntasks;
                  futures.add(ecs.submit(it.next()));
                  ++active;
               } else if (active == 0)
                  break;
               else if (timed) {
                  f = ecs.poll(nanos, TimeUnit.NANOSECONDS);
                  if (f == null)
                     throw new TimeoutException();
                  long now = System.nanoTime();
                  nanos -= now - lastTime;
                  lastTime = now;
               } else
                  f = ecs.take();
            }
            if (f != null) {
               --active;
               try {
                  return f.get();
               } catch (InterruptedException ie) {
                  throw ie;
               } catch (ExecutionException eex) {
                  ee = eex;
               } catch (RuntimeException rex) {
                  ee = new ExecutionException(rex);
               }
            }
         }

         if (ee == null)
            ee = new ExecutionException() {
               private static final long serialVersionUID = 200818694545553992L;
            };
         throw ee;

      } finally {
         for (Future<T> f : futures)
            f.cancel(true);
      }
   }

   @Override
   public void execute(Runnable command) {
      if (!isShutdown.get()) {
         DistributedTaskPart<Object> cmd;
         if (command instanceof DistributedTaskPart<?>) {
            cmd = (DistributedTaskPart<Object>) command;
         } else if (command instanceof Serializable) {
            cmd = (DistributedTaskPart<Object>) newTaskFor(command, null);
         } else {
            throw new IllegalArgumentException("Runnable command is not Serializable  " + command);
         }
         execute(selectExecutionNode(), cmd);
      } else {
         throw new RejectedExecutionException();
      }
   }

   @Override
   protected <T> RunnableFuture<T> newTaskFor(Runnable runnable, T value) {
      if (runnable == null) throw new NullPointerException();
      RunnableAdapter<T> adapter = new RunnableAdapter<T>(runnable, value);
      return newTaskFor(adapter);
   }

   @Override
   protected <T> RunnableFuture<T> newTaskFor(Callable<T> callable) {
      if (callable == null) throw new NullPointerException();
      DistributedTaskBuilder<T> distributedTaskBuilder = getDistributedTaskBuilder();
      DistributedTask<T> distributedTask = distributedTaskBuilder.callable(callable).build();
      DistributedExecuteCommand<T> executeCommand = factory.buildDistributedExecuteCommand(
               callable, getAddress(), null);
      return createDistributedTaskPart(distributedTask, executeCommand, selectExecutionNode());
   }

   @Override
   public <T> Future<T> submit(Address target, Callable<T> task) {
      DistributedTaskBuilder<T> distributedTaskBuilder = getDistributedTaskBuilder();
      DistributedTask<T> distributedTask = distributedTaskBuilder.callable(task).build();
      return submit(target, distributedTask);
   }

   public <T> Future<T> submit(Address target, DistributedTask<T> task) {
      if (task == null)
         throw new NullPointerException();
      if (target == null)
         throw new NullPointerException();
      List<Address> members = executionCandidates();
      if (!members.contains(target)) {
         throw new IllegalArgumentException("Target node " + target
                  + " is not a cluster member, members are " + members);
      }
      Address me = getAddress();
      DistributedExecuteCommand<T> c = null;
      if (target.equals(me)) {
         c = factory.buildDistributedExecuteCommand(clone(task.getCallable()), me, null);
      } else {
         c = factory.buildDistributedExecuteCommand(task.getCallable(), me, null);
      }
      DistributedTaskPart<T> f = createDistributedTaskPart(task, c, target);
      execute(target, f);
      return f;
   }

   @Override
   public <T, K> Future<T> submit(Callable<T> task, K... input) {
      DistributedTaskBuilder<T> distributedTaskBuilder = getDistributedTaskBuilder();
      DistributedTask<T> distributedTask = distributedTaskBuilder.callable(task).build();
      return submit(distributedTask, input);
   }

   public <T, K> Future<T> submit(DistributedTask<T> task, K... input) {
      if (task == null) throw new NullPointerException();

      if(inputKeysSpecified(input)){
         Map<Address, List<K>> nodesKeysMap = task.getTaskExecutionPolicy().keysToExecutionNodes(input);     
         checkExecutionPolicy(task, nodesKeysMap, input);
         Address me = getAddress();
         DistributedExecuteCommand<T> c = factory.buildDistributedExecuteCommand(task.getCallable(), me, Arrays.asList(input));         
         ArrayList<Address> nodes = new ArrayList<Address>(nodesKeysMap.keySet());
         DistributedTaskPart<T> f = createDistributedTaskPart(task, c, selectExecutionNode(nodes));
         execute(f);
         return f;
      } else {
         return submit(task.getCallable());
      }
   }

   @Override
   public <T> List<Future<T>> submitEverywhere(Callable<T> task) {
      DistributedTaskBuilder<T> distributedTaskBuilder = getDistributedTaskBuilder();
      DistributedTask<T> distributedTask = distributedTaskBuilder.callable(task).build();
      return submitEverywhere(distributedTask);
   }

   public <T> List<Future<T>> submitEverywhere(DistributedTask<T> task) {
      if (task == null) throw new NullPointerException();
      List<Address> members = executionCandidates();
      List<Future<T>> futures = new ArrayList<Future<T>>(members.size() - 1);      
      Address me = getAddress();
      for (Address target : members) {
         DistributedExecuteCommand<T> c = null;
         if (target.equals(me)) {
            c = factory.buildDistributedExecuteCommand(clone(task.getCallable()), me, null);
         } else {
            c = factory.buildDistributedExecuteCommand(task.getCallable(), me, null);
         }
         DistributedTaskPart<T> f = createDistributedTaskPart(task, c, target);
         futures.add(f);
         execute(f);
      }
      return futures;
   }

   @Override
   public <T, K> List<Future<T>> submitEverywhere(Callable<T> task, K... input) {
      DistributedTaskBuilder<T> distributedTaskBuilder = getDistributedTaskBuilder();
      DistributedTask<T> distributedTask = distributedTaskBuilder.callable(task).build();
      return submitEverywhere(distributedTask, input);      
   } 

   public <T, K> List<Future<T>> submitEverywhere(DistributedTask<T> task, K... input) {
      if (task == null) throw new NullPointerException();
      if(inputKeysSpecified(input)) {
         List<Future<T>> futures = new ArrayList<Future<T>>(input.length * 2);
         Address me = getAddress();
         Map<Address, List<K>> nodesKeysMap = task.getTaskExecutionPolicy().keysToExecutionNodes(input);
         checkExecutionPolicy(task, nodesKeysMap, input);
         for (Entry<Address, List<K>> e : nodesKeysMap.entrySet()) {
            Address target = e.getKey();
            DistributedExecuteCommand<T> c = null;
            if (target.equals(me)) {
               c = factory.buildDistributedExecuteCommand(clone(task.getCallable()), me, e.getValue());
            } else {
               c = factory.buildDistributedExecuteCommand(task.getCallable(), me, e.getValue());
            }
            DistributedTaskPart<T> f = createDistributedTaskPart(task, c, target);
            futures.add(f);
            execute(f);
         }
         return futures;
      } else {
         return submitEverywhere(task);
      }
   }

   protected <T> Callable<T> clone(Callable<T> task){
     return Util.cloneWithMarshaller(marshaller, task);
   }

   protected <T> DistributedTaskPart<T> createDistributedTaskPart(DistributedTask<T> task,
            DistributedExecuteCommand<T> c, Address target) {
      Address executionTargetSelected = task.getTaskExecutionPolicy().executionTargetSelected(
               target, executionCandidates());

      if (executionTargetSelected == null)
         throw new IllegalStateException("Invalid execution target " + executionTargetSelected
                  + " returned for task " + task + " by its " + task.getTaskExecutionPolicy()
                  + " DistributedTaskExecutionPolicy");
      return new DefaultDistributedTaskPart<T>(task, c, executionTargetSelected);
   }
   
   private <T, K> void checkExecutionPolicy(DistributedTask<T> task,
            Map<Address, List<K>> nodesKeysMap, K... input) {
      if (nodesKeysMap == null || nodesKeysMap.isEmpty()) {
         throw new IllegalStateException("DistributedTaskExecutionPolicy "
                  + task.getTaskExecutionPolicy() + " for task " + task
                  + " returned invalid keysToExecutionNodes " + nodesKeysMap
                  + " execution policy plan for a given input " + input);
      }
   }

   protected <T> void execute(Address target, DistributedTaskPart<T> f) {
      if (getAddress().equals(target)) {
         invokeLocally(f);
      } else {
         log.tracef("Sending %s to remote execution at node %s", f, f.getExecutionTarget());
         try {
            rpc.invokeRemotelyInFuture(Collections.singletonList(target), f.getCommand(), false,
                     (DistributedTaskPart<Object>) f, f.getOwningTask().timeout());
         } catch (Throwable e) {
            log.remoteExecutionFailed(target, e);
         }
      }
   }
   
   protected <T> void execute(DistributedTaskPart<T> f) {
      execute(f.getExecutionTarget(), f);
   }

   private <K> boolean inputKeysSpecified(K...input){
      return input != null && input.length > 0;
   }

   protected <T> void invokeLocally(final DistributedTaskPart<T> future) {
      log.debugf("Sending %s to self", future);
      try {
         Callable<Object> call = new Callable<Object>() {

            @Override
            public Object call() throws Exception {
               Object result = null;
               future.getCommand().init(cache);
               DistributedTaskLifecycleService taskLifecycleService = DistributedTaskLifecycleService.getInstance();
               try {
                  //hook into lifecycle
                  taskLifecycleService.onPreExecute(future.getCommand().getCallable(),cache);
                  result = future.getCommand().perform(null);
                  return Collections.singletonMap(getAddress(), SuccessfulResponse.create(result));
               } catch (Throwable e) {
                  return e;
               } finally {
                  //hook into lifecycle
                  taskLifecycleService.onPostExecute(future.getCommand().getCallable());
                  future.notifyDone();
               }
            }
         };
         final FutureTask<Object> task = new FutureTask<Object>(call);
         future.setNetworkFuture((Future<T>) task);
         localExecutorService.submit(task);
      } catch (Throwable e1) {
         log.localExecutionFailed(e1);
      }
   }

   protected Address selectExecutionNode(List<Address> candidates) {
      List<Address> list = randomClusterMembers(candidates,1);
      return list.get(0);
   }

   protected Address selectExecutionNode() {
     return selectExecutionNode(executionCandidates());
   }

   protected List<Address> randomClusterMembers(final List<Address> members, int numNeeded) {
      if(members == null || members.isEmpty())
         throw new IllegalArgumentException("Invalid member list " + members);

      if (members.size() < numNeeded) {
         log.cannotSelectRandomMembers(numNeeded, members);
         numNeeded = members.size();
      }
      List<Address> membersCopy = new ArrayList<Address>(members);
      List<Address> chosen = new ArrayList<Address>(numNeeded);
      Random r = new Random();
      while (!membersCopy.isEmpty() && numNeeded >= chosen.size()) {
         int count = membersCopy.size();
         Address address = membersCopy.remove(r.nextInt(count));
         chosen.add(address);
      }
      return chosen;
   }

   private void ensureProperCacheState(AdvancedCache<?, ?> cache) throws NullPointerException,
            IllegalStateException {

      if (cache.getRpcManager() == null)
         throw new IllegalStateException("Can not use non-clustered cache for DefaultExecutorService");

      if (cache.getStatus() != ComponentStatus.RUNNING)
         throw new IllegalStateException("Invalid cache state " + cache.getStatus());
   }
   
   private class DefaultTaskExecutionPolicy implements DistributedTaskExecutionPolicy{

      @Override
      public <K> Map<Address, List<K>> keysToExecutionNodes(K... input) {

         DistributionManager dm = cache.getDistributionManager();
         Map<Address, List<K>> addressToKey = new HashMap<Address, List<K>>(input.length * 2);
         boolean usingREPLMode = dm == null;
         List<Address> members = null;
         if (usingREPLMode) {
            members = new ArrayList<Address>(cache.getRpcManager().getTransport().getMembers());
         }
         for (K key : input) {
            Address ownerOfKey = null;
            if (usingREPLMode) {
               // using REPL mode https://issues.jboss.org/browse/ISPN-1886
               // since keys and values are on all nodes, lets just pick randomly
               Collections.shuffle(members);
               ownerOfKey = members.get(0);
            } else {
               // DIST mode
               ownerOfKey = dm.getPrimaryLocation(key);
            }
            List<K> keysAtNode = addressToKey.get(ownerOfKey);
            if (keysAtNode == null) {
               keysAtNode = new LinkedList<K>();
               addressToKey.put(ownerOfKey, keysAtNode);
            }
            keysAtNode.add(key);
         }
         return addressToKey;
      }

      @Override
      public Address executionTargetSelected(Address executionTarget, List<Address> candidates) {
         //simply return the already selected node
         return executionTarget;
      }  
   }

   private class DefaultDistributedTaskBuilder<T> implements DistributedTaskBuilder<T>, DistributedTask<T>{      

      private Callable<T> callable;
      private DistributedTaskExecutionPolicy executionPolicy;
      private long timeout;

      public DefaultDistributedTaskBuilder(DistributedExecutorService service, long taskTimeout) {
         this.executionPolicy = new DefaultTaskExecutionPolicy();
         this.timeout = taskTimeout;
      }

      public DefaultDistributedTaskBuilder(long taskTimeout) {
         this.timeout = taskTimeout;
      }

      @Override
      public DistributedTaskBuilder<T> callable(Callable<T> callable) {
         if (callable == null)
            throw new IllegalArgumentException("Callable cannot be null");
         this.callable = callable;
         return this;
      }

      @Override
      public DistributedTaskBuilder<T> timeout(long t, TimeUnit tu) {
         timeout = TimeUnit.MILLISECONDS.convert(timeout, tu);
         return this;
      }

      @Override
      public DistributedTaskBuilder<T> executionPolicy(DistributedTaskExecutionPolicy policy) {
         if (policy == null)
            throw new IllegalArgumentException("DistributedTaskExecutionPolicy cannot be null");
         
         this.executionPolicy = policy;
         return this;
      }

      @Override
      public DistributedTask<T> build() {
         DefaultDistributedTaskBuilder<T> task = new DefaultDistributedTaskBuilder<T>(timeout);
         task.callable(callable);
         task.executionPolicy(executionPolicy);
         return task;         
      }

      @Override
      public long timeout() {
         return timeout;
      }

      @Override
      public DistributedTaskExecutionPolicy getTaskExecutionPolicy() {
         return executionPolicy;         
      }

      @Override
      public Callable<T> getCallable() {
         return callable;
      }      
   }
   
   /**
    * A partial unit of the entire DistributedTask, a single unit of execution
    */
   public interface DistributedTaskPart<V> extends NotifyingNotifiableFuture<V>, RunnableFuture<V> {

      DistributedExecuteCommand<V> getCommand();

      Address getExecutionTarget();

      DistributedTask<V> getOwningTask();
   }


   /**
    * DefaultDistributedTaskPart is essentially a Future wrap around DistributedExecuteCommand.
    *
    *
    * @author Mircea Markus
    * @author Vladimir Blagojevic
    * 
    * TODO Add state transitions EXECUTED, FAILEDOVER etc 
    */
   private class DefaultDistributedTaskPart<V> implements DistributedTaskPart<V> {

      private final DistributedExecuteCommand<V> distCommand;
      private volatile Future<V> f;
      //TODO revisit if volatile needed
      private volatile boolean callCompleted = false;
      private final Set<FutureListener<V>> listeners = new CopyOnWriteArraySet<FutureListener<V>>();
      private final ReadWriteLock listenerLock = new ReentrantReadWriteLock();
      private final Address executionTarget;
      private final DistributedTask<V> owningTask;

      /**
       * Creates a <tt>DistributedRunnableFuture</tt> that will upon running, execute the given
       * <tt>Runnable</tt>, and arrange that <tt>get</tt> will return the given result on successful
       * completion.
       *
       *
       * @param runnable
       *           the runnable task
       * @param result
       *           the result to return on successful completion.
       *
       */
      public DefaultDistributedTaskPart(DistributedTask<V> task, DistributedExecuteCommand<V> command, Address executionTarget) {
         this.owningTask = task;
         this.distCommand = command;  
         this.executionTarget = executionTarget;
      }
 
      public DistributedExecuteCommand<V> getCommand() {
         return distCommand;
      }

      @Override
      public DistributedTask<V> getOwningTask() {
         return owningTask;
      }

      @Override
      public Address getExecutionTarget() {
         return executionTarget;      
      }

      public boolean isCancelled() {
         return f.isCancelled();
      }

      public boolean isDone() {
         return f.isDone();
      }

      public boolean cancel(boolean mayInterruptIfRunning) {
         return f.cancel(mayInterruptIfRunning);
      }

      /**
       *
       */
      public V get() throws InterruptedException, ExecutionException {
         V response = f.get();
         return retrieveResult(response);
      }

      /**
       *
       */
      public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException,
               TimeoutException {
         V response = f.get(timeout,unit);
         return retrieveResult(response);
      }

      public void notifyDone() {
         listenerLock.writeLock().lock();
         try {
            callCompleted = true;
            for (FutureListener<V> l : listeners) l.futureDone(this);
         } finally {
            listenerLock.writeLock().unlock();
         }
      }

      public NotifyingFuture<V> attachListener(FutureListener<V> listener) {
         listenerLock.readLock().lock();
         try {
            if (!callCompleted) listeners.add(listener);
            if (callCompleted) listener.futureDone(this);
            return this;
         } finally {
            listenerLock.readLock().unlock();
         }
      }

      @Override
      public void setNetworkFuture(Future<V> future) {
         this.f = future;
      }

      V retrieveResult(Object response) throws ExecutionException {
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
      public int hashCode() {
         final int prime = 31;
         int result = 1;
         result = prime * result + getOuterType().hashCode();
         result = prime * result + (callCompleted ? 1231 : 1237);
         result = prime * result + ((distCommand == null) ? 0 : distCommand.hashCode());
         return result;
      }

      @Override
      public boolean equals(Object obj) {
         if (this == obj) {
            return true;
         }
         if (obj == null) {
            return false;
         }
         if (!(obj instanceof DefaultDistributedTaskPart)) {
            return false;
         }
         DefaultDistributedTaskPart other = (DefaultDistributedTaskPart) obj;
         if (!getOuterType().equals(other.getOuterType())) {
            return false;
         }
         if (callCompleted != other.callCompleted) {
            return false;
         }
         if (distCommand == null) {
            if (other.distCommand != null) {
               return false;
            }
         } else if (!distCommand.equals(other.distCommand)) {
            return false;
         }
         return true;
      }

      @Override
      public void run() {
         //intentionally empty
      }

      private DefaultExecutorService getOuterType() {
         return DefaultExecutorService.this;
      }
   }
   private static final class RunnableAdapter<T> implements Callable<T>, Serializable {

      /** The serialVersionUID */
      private static final long serialVersionUID = 6629286923873531028L;

      protected Runnable task;
      protected T result;

      protected RunnableAdapter() {
      }

      protected RunnableAdapter(Runnable task, T result) {
         this.task = task;
         this.result = result;
      }

      @Override
      public T call() {
         task.run();
         return result;
      }
   }
}
