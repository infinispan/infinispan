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
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionService;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commands.CancelCommand;
import org.infinispan.commands.CancellationService;
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
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.TopologyAwareAddress;
import org.infinispan.util.InfinispanCollections;
import org.infinispan.util.Util;
import org.infinispan.util.concurrent.FutureListener;
import org.infinispan.util.concurrent.NotifyingFuture;
import org.infinispan.util.concurrent.NotifyingNotifiableFuture;
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

   private static final NodeFilter SAME_MACHINE_FILTER = new NodeFilter(){
      @Override
      public boolean include(TopologyAwareAddress thisAddress, TopologyAwareAddress otherAddress) {
         return thisAddress.isSameMachine(otherAddress);
      };
   };

   private static final NodeFilter SAME_RACK_FILTER = new NodeFilter(){
      @Override
      public boolean include(TopologyAwareAddress thisAddress, TopologyAwareAddress otherAddress) {
         return thisAddress.isSameRack(otherAddress);
      };
   };

   private static final NodeFilter SAME_SITE_FILTER = new NodeFilter(){
      @Override
      public boolean include(TopologyAwareAddress thisAddress, TopologyAwareAddress otherAddress) {
         return thisAddress.isSameSite(otherAddress);
      };
   };

   private static final NodeFilter ALL_FILTER = new NodeFilter(){
      @Override
      public boolean include(TopologyAwareAddress thisAddress, TopologyAwareAddress otherAddress) {
         return true;
      };
   };
   
   public static final Address LOCAL_MODE_ADDRESS = new Address() {
      
      @Override
      public int compareTo(Address o) {
         return 0;
      }
   };
   
   public static final DistributedTaskFailoverPolicy NO_FAILOVER = new NoTaskFailoverPolicy();
   public static final DistributedTaskFailoverPolicy RANDOM_NODE_FAILOVER = new RandomNodeTaskFailoverPolicy();

   private static final Log log = LogFactory.getLog(DefaultExecutorService.class);
   private static final boolean trace = log.isTraceEnabled();
   protected final AtomicBoolean isShutdown = new AtomicBoolean(false);
   protected final AdvancedCache cache;
   protected final RpcManager rpc;
   protected final InterceptorChain invoker;
   protected final CommandsFactory factory;
   protected final Marshaller marshaller;
   protected final ExecutorService localExecutorService;
   protected final CancellationService cancellationService;
   protected final boolean takeExecutorOwnership;

   /**
    * Creates a new DefaultExecutorService given a master cache node for local task execution. All
    * distributed task executions will be initiated from this Infinispan cache node
    *
    * @param masterCacheNode
    *           Cache node initiating distributed task
    */
   public DefaultExecutorService(Cache<?, ?> masterCacheNode) {
      this(masterCacheNode, Executors.newSingleThreadExecutor(), true);
   }

   /**
    * Creates a new DefaultExecutorService given a master cache node and an ExecutorService for
    * parallel execution of tasks ran on this node. All distributed task executions will be
    * initiated from this Infinispan cache node.
    * <p>
    * Note that DefaultExecutorService will not shutdown client supplied localExecutorService once
    * this DefaultExecutorService is shutdown. Lifecycle management of a supplied ExecutorService is
    * left to the client
    * 
    * Also note that client supplied ExecutorService should not execute tasks in the caller's thread
    * ( i.e rejectionHandler of {@link ThreadPoolExecutor} configured with {link
    * {@link ThreadPoolExecutor.CallerRunsPolicy})
    * 
    * @param masterCacheNode
    *           Cache node initiating distributed task
    * @param localExecutorService
    *           ExecutorService to run local tasks
    */
   public DefaultExecutorService(Cache<?, ?> masterCacheNode, ExecutorService localExecutorService) {
      this(masterCacheNode, localExecutorService, false);
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
    * @param takeExecutorOwnership
    *           if true {@link DistributedExecutorService#shutdown()} and
    *           {@link DistributedExecutorService#shutdownNow()} method will shutdown
    *           localExecutorService as well
    * 
    */
   public DefaultExecutorService(Cache<?, ?> masterCacheNode, ExecutorService localExecutorService,
            boolean takeExecutorOwnership) {
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
      this.cancellationService = registry.getComponent(CancellationService.class);
      this.localExecutorService = localExecutorService;
      this.takeExecutorOwnership = takeExecutorOwnership;
   }

   @Override
   public <T> DistributedTaskBuilder<T> createDistributedTaskBuilder(Callable<T> callable) {
      long to = cache.getCacheConfiguration().clustering().sync().replTimeout();
      DistributedTaskBuilder<T> dtb = new DefaultDistributedTaskBuilder<T>(to);
      dtb.callable(callable);
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

   protected List<Address> getMembers() {
      if (rpc != null) {
         return rpc.getMembers();
      } else {
         return Collections.singletonList(getAddress());
      }
   }

   protected <T> List<Address> executionCandidates(DistributedTask<T> task) {
      return filterMembers(task.getTaskExecutionPolicy(), getMembers());
   }

   private Address getAddress() {
      if (rpc != null) {
         return rpc.getAddress();
      } else {
         return LOCAL_MODE_ADDRESS;
      }
   }

   private List<Runnable> realShutdown(boolean interrupt) {
      isShutdown.set(true);
      // TODO cancel all tasks
      if (takeExecutorOwnership) {
         if (interrupt)
            localExecutorService.shutdownNow();
         else
            localExecutorService.shutdown();
      }
      return InfinispanCollections.emptyList();
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
         cmd.execute();
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
      DistributedTaskBuilder<T> distributedTaskBuilder = createDistributedTaskBuilder(callable);
      DistributedTask<T> task = distributedTaskBuilder.build();
      DistributedExecuteCommand<T> executeCommand = factory.buildDistributedExecuteCommand(
               callable, getAddress(), null);
      return createDistributedTaskPart(task, executeCommand, selectExecutionNode(task), 0);
   }

   @Override
   public <T> Future<T> submit(Address target, Callable<T> task) {
      DistributedTaskBuilder<T> distributedTaskBuilder = createDistributedTaskBuilder(task);
      DistributedTask<T> distributedTask = distributedTaskBuilder.build();
      return submit(target, distributedTask);
   }

   @Override
   public <T> Future<T> submit(Address target, DistributedTask<T> task) {
      if (task == null)
         throw new NullPointerException();
      if (target == null)
         throw new NullPointerException();
      List<Address> members = getMembers();
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
      DistributedTaskPart<T> part = createDistributedTaskPart(task, c, target, 0);
      part.execute();
      return part;
   }

   @Override
   public <T, K> Future<T> submit(Callable<T> task, K... input) {
      DistributedTaskBuilder<T> distributedTaskBuilder = createDistributedTaskBuilder(task);
      DistributedTask<T> distributedTask = distributedTaskBuilder.build();
      return submit(distributedTask, input);
   }

   @Override
   public <T, K> Future<T> submit(DistributedTask<T> task, K... input) {
      if (task == null) throw new NullPointerException();

      if(inputKeysSpecified(input)){
         Map<Address, List<K>> nodesKeysMap = keysToExecutionNodes(task.getTaskExecutionPolicy(), input);
         checkExecutionPolicy(task, nodesKeysMap, input);
         Address me = getAddress();
         DistributedExecuteCommand<T> c = factory.buildDistributedExecuteCommand(task.getCallable(), me, Arrays.asList(input));
         ArrayList<Address> nodes = new ArrayList<Address>(nodesKeysMap.keySet());
         DistributedTaskPart<T> part = createDistributedTaskPart(task, c, Arrays.asList(input), selectExecutionNode(nodes), 0);
         part.execute();
         return part;
      } else {
         return submit(task.getCallable());
      }
   }

   @Override
   public <T> List<Future<T>> submitEverywhere(Callable<T> task) {
      DistributedTaskBuilder<T> distributedTaskBuilder = createDistributedTaskBuilder(task);
      DistributedTask<T> distributedTask = distributedTaskBuilder.build();
      return submitEverywhere(distributedTask);
   }

   @Override
   public <T> List<Future<T>> submitEverywhere(DistributedTask<T> task) {
      if (task == null) throw new NullPointerException();

      List<Address> members = executionCandidates(task);
      List<Future<T>> futures = new ArrayList<Future<T>>(members.size());
      Address me = getAddress();
      for (Address target : members) {
         DistributedExecuteCommand<T> c = null;
         if (target.equals(me)) {
            c = factory.buildDistributedExecuteCommand(clone(task.getCallable()), me, null);
         } else {
            c = factory.buildDistributedExecuteCommand(task.getCallable(), me, null);
         }
         DistributedTaskPart<T> part = createDistributedTaskPart(task, c, target, 0);
         futures.add(part);
         part.execute();
      }
      return futures;
   }

   @Override
   public <T, K> List<Future<T>> submitEverywhere(Callable<T> task, K... input) {
      DistributedTaskBuilder<T> distributedTaskBuilder = createDistributedTaskBuilder(task);
      DistributedTask<T> distributedTask = distributedTaskBuilder.build();
      return submitEverywhere(distributedTask, input);
   }

   @Override
   public <T, K> List<Future<T>> submitEverywhere(DistributedTask<T> task, K... input) {
      if (task == null) throw new NullPointerException();
      if(inputKeysSpecified(input)) {
         List<Future<T>> futures = new ArrayList<Future<T>>(input.length * 2);
         Address me = getAddress();
         Map<Address, List<K>> nodesKeysMap = keysToExecutionNodes(task.getTaskExecutionPolicy(), input);
         checkExecutionPolicy(task, nodesKeysMap, input);
         for (Entry<Address, List<K>> e : nodesKeysMap.entrySet()) {
            Address target = e.getKey();
            DistributedExecuteCommand<T> c = null;
            if (target.equals(me)) {
               c = factory.buildDistributedExecuteCommand(clone(task.getCallable()), me, e.getValue());
            } else {
               c = factory.buildDistributedExecuteCommand(task.getCallable(), me, e.getValue());
            }            
            DistributedTaskPart<T> part = createDistributedTaskPart(task, c, e.getValue(), target, 0);
            futures.add(part);
            part.execute();
         }
         return futures;
      } else {
         return submitEverywhere(task);
      }
   }

   protected <T> Callable<T> clone(Callable<T> task){
     return Util.cloneWithMarshaller(marshaller, task);
   }

   protected <T, K> DistributedTaskPart<T> createDistributedTaskPart(DistributedTask<T> task,
            DistributedExecuteCommand<T> c, List<K> inputKeys, Address target,
            int failoverCount) {
      return new DistributedTaskPart<T>(task, c, (List<Object>) inputKeys, target, failoverCount);
   }
   
   protected <T, K> DistributedTaskPart<T> createDistributedTaskPart(DistributedTask<T> task,
            DistributedExecuteCommand<T> c, Address target, int failoverCount) {
      return createDistributedTaskPart(task, c, Collections.emptyList(), target, failoverCount);
   }


   private <T, K> void checkExecutionPolicy(DistributedTask<T> task,
            Map<Address, List<K>> nodesKeysMap, K... input) {
      if (nodesKeysMap == null || nodesKeysMap.isEmpty()) {
         throw new IllegalStateException("DistributedTaskExecutionPolicy "
                  + task.getTaskExecutionPolicy() + " for task " + task
                  + " returned invalid keysToExecutionNodes " + nodesKeysMap
                  + " execution policy plan for a given input " + Arrays.toString(input));
      }
   }

   private <K> boolean inputKeysSpecified(K...input){
      return input != null && input.length > 0;
   }

   protected Address selectExecutionNode(List<Address> candidates) {
      List<Address> list = randomClusterMembers(candidates,1);
      return list.get(0);
   }

   protected <T> Address selectExecutionNode(DistributedTask <T> task) {
     return selectExecutionNode(executionCandidates(task));
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

   protected <K> Map<Address, List<K>> keysToExecutionNodes(DistributedTaskExecutionPolicy policy, K... input) {
      DistributionManager dm = cache.getDistributionManager();
      Map<Address, List<K>> addressToKey = new HashMap<Address, List<K>>(input.length * 2);
      boolean usingREPLMode = dm == null;
      for (K key : input) {
         Address ownerOfKey = null;
         if (usingREPLMode) {
            List<Address> members = new ArrayList<Address>(getMembers());
            members =  filterMembers(policy, members);
            // using REPL mode https://issues.jboss.org/browse/ISPN-1886
            // since keys and values are on all nodes, lets just pick randomly
            Collections.shuffle(members);
            ownerOfKey = members.get(0);
         } else {
            // DIST mode
            List<Address> owners = dm.locate(key);
            List<Address> filtered = filterMembers(policy, owners);
            if(!filtered.isEmpty()){
               ownerOfKey = filtered.get(0);
            } else {
               ownerOfKey = owners.get(0);
            }
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

   private List<Address> filterMembers(DistributedTaskExecutionPolicy policy, List<Address> members) {
      NodeFilter filter = null;
      switch (policy) {
         case SAME_MACHINE:
            filter = SAME_MACHINE_FILTER;
            break;
         case SAME_SITE:
            filter = SAME_SITE_FILTER;
            break;
         case SAME_RACK:
            filter = SAME_RACK_FILTER;
            break;
         case ALL:
            filter = ALL_FILTER;
            break;
         default:
            filter = ALL_FILTER;
            break;
      }
      List<Address> result = new ArrayList<Address>();
      for (Address address : members) {
         if(address instanceof TopologyAwareAddress){
            TopologyAwareAddress taa = (TopologyAwareAddress)address;
            if(filter.include(taa, (TopologyAwareAddress)getAddress())){
               result.add(address);
            }
         } else {
            result.add(address);
         }
      }
      return result;
   }

   private void ensureProperCacheState(AdvancedCache<?, ?> cache) throws NullPointerException,
            IllegalStateException {

      if (cache.getStatus() != ComponentStatus.RUNNING)
         throw new IllegalStateException("Invalid cache state " + cache.getStatus());
   }

   private static class RandomNodeTaskFailoverPolicy implements DistributedTaskFailoverPolicy {

      public RandomNodeTaskFailoverPolicy() {
         super();
      }

      @Override
      public Address failover(FailoverContext fc) {
         return randomNode(fc.executionCandidates(),fc.executionFailureLocation());
      }

      protected Address randomNode(List<Address> candidates, Address failedExecutionLocation){
         Random r = new Random();
         candidates.remove(failedExecutionLocation);
         if (candidates.isEmpty())
            throw new IllegalStateException("There are no candidates for failover: " + candidates);
         int tIndex = r.nextInt(candidates.size());
         return candidates.get(tIndex);
      }

      @Override
      public int maxFailoverAttempts() {
         return 1;
      }
   }
   
   private static class NoTaskFailoverPolicy implements DistributedTaskFailoverPolicy {

      public NoTaskFailoverPolicy() {
         super();
      }

      @Override
      public Address failover(FailoverContext fc) {
         return fc.executionFailureLocation();
      }

      @Override
      public int maxFailoverAttempts() {
         return 0;
      }
   }

   /**
    * NodeFilter allows selection of nodes according to {@link DistributedTaskExecutionPolicy}
    */
   interface NodeFilter {
      boolean include(TopologyAwareAddress thisAddress, TopologyAwareAddress otherAddress);
   }

   private class DefaultDistributedTaskBuilder<T> implements DistributedTaskBuilder<T>, DistributedTask<T>{

      private Callable<T> callable;
      private long timeout;
      private DistributedTaskExecutionPolicy executionPolicy = DistributedTaskExecutionPolicy.ALL;
      private DistributedTaskFailoverPolicy failoverPolicy = NO_FAILOVER;


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
         timeout = TimeUnit.MILLISECONDS.convert(t, tu);
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
      public DistributedTaskBuilder<T> failoverPolicy(DistributedTaskFailoverPolicy policy) {
         if (policy == null) {
            this.failoverPolicy = NO_FAILOVER;
         } else {
            this.failoverPolicy = policy;
         }
         return this;
      }

      @Override
      public DistributedTask<T> build() {
         DefaultDistributedTaskBuilder<T> task = new DefaultDistributedTaskBuilder<T>(timeout);
         task.callable(callable);
         task.executionPolicy(executionPolicy);
         task.failoverPolicy(failoverPolicy);
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
      public DistributedTaskFailoverPolicy getTaskFailoverPolicy() {
         return failoverPolicy;
      }

      @Override
      public Callable<T> getCallable() {
         return callable;
      }
   }

   /**
    * DistributedTaskPart represents a unit of work sent to remote VM and executed there
    *
    *
    * @author Mircea Markus
    * @author Vladimir Blagojevic
    */
   private class DistributedTaskPart<V> implements NotifyingNotifiableFuture<V>, RunnableFuture<V>{

      private final DistributedExecuteCommand<V> distCommand;
      private volatile Future<V> f;
      //TODO revisit if volatile needed
      private volatile boolean callCompleted = false;
      private final Set<FutureListener<V>> listeners = new CopyOnWriteArraySet<FutureListener<V>>();
      private final ReadWriteLock listenerLock = new ReentrantReadWriteLock();
      private final Address executionTarget;
      private final List<Object> inputKeys; 
      private final DistributedTask<V> owningTask;
      private int failedOverCount;
      private volatile boolean done;
      private volatile boolean cancelled;


      /**
       * Create a new DistributedTaskPart.
       * 
       * @param task
       * @param command
       * @param executionTarget
       * @param failoverCount
       */
      public DistributedTaskPart(DistributedTask<V> task, DistributedExecuteCommand<V> command,
               List<Object> inputKeys, Address executionTarget, int failoverCount) {
         this.owningTask = task;
         this.distCommand = command;
         this.inputKeys = inputKeys;
         this.executionTarget = executionTarget;
         this.failedOverCount = failoverCount;
      }

      public List<Object> getInputKeys() {
         return inputKeys;
      }

      public DistributedExecuteCommand<V> getCommand() {
         return distCommand;
      }

      public DistributedTask<V> getOwningTask() {
         return owningTask;
      }


      public Address getExecutionTarget() {
         return executionTarget;
      }

      @Override
      public boolean isCancelled() {
         return cancelled;
      }

      @Override
      public boolean isDone() {
         return done;
      }
      
      public boolean isLocalNodeExecutionTarget(){
         return getAddress().equals(getExecutionTarget());
      }

      @Override
      public boolean cancel(boolean mayInterruptIfRunning) {
         if (!isCancelled()) {          
            CancelCommand ccc = factory.buildCancelCommandCommand(distCommand.getUUID());
            if (isLocalNodeExecutionTarget()) {
               ccc.init(cancellationService);
               try {
                  ccc.perform(null);
               } catch (Throwable e) {
                  log.couldNotExecuteCancellationLocally(e.getLocalizedMessage());
               }
            } else {
               rpc.invokeRemotely(Collections.singletonList(getExecutionTarget()), ccc, rpc.getDefaultRpcOptions(true));
            }
            cancelled = true;
            done = true;
            return cancelled;
         } else {
            //already cancelled
            return false;
         }
      }

      /**
       *
       */
      @Override
      public V get() throws InterruptedException, ExecutionException {
         V result = null;
         try {
            result = innerGet(0, TimeUnit.MILLISECONDS);
         } catch (TimeoutException e) {
            throw new ExecutionException(e);
         } finally {
            done = true;
         }
         return result;
      }

      /**
       *
       */
      @Override
      public V get(long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
         try {
            return innerGet(timeout, unit);
         } finally {
            done = true;
         }
      }
      
      private V innerGet(long timeout, TimeUnit unit)
            throws ExecutionException, TimeoutException, InterruptedException {
         if (isCancelled())
            throw new CancellationException("Task already cancelled");

         long timeoutNanos = computeTimeoutNanos(timeout, unit);
         long endNanos = System.nanoTime() + timeoutNanos;
         V response;
         try {
            if (timeoutNanos > 0) {
               response = retrieveResult(f.get(timeoutNanos, TimeUnit.NANOSECONDS));
            } else {               
               response = retrieveResult(f.get());
            }
         } catch (TimeoutException te) {
            throw te;
         } catch (Exception e) {
            // The RPC could have finished with a org.infinispan.util.concurrent.TimeoutException right before
            // the Future.get timeout expired. If that's the case, we want to throw a TimeoutException.
            long remainingNanos = timeoutNanos > 0 ? endNanos - System.nanoTime() : timeoutNanos;
            if (timeoutNanos > 0 && remainingNanos <= 0) {
               if (trace) log.tracef("Distributed task timed out, throwing a TimeoutException and ignoring exception", e);
               throw new TimeoutException();
            }
            boolean canFailover = failedOverCount++ < getOwningTask().getTaskFailoverPolicy().maxFailoverAttempts();
            if (canFailover) {
               try {
                  response = failoverExecution(e, timeoutNanos, TimeUnit.NANOSECONDS);
               } catch (Exception failedOver) {
                  throw wrapIntoExecutionException(failedOver);
               }
            } else {
               throw wrapIntoExecutionException(e);
            }
         }
         return response;
      }

      private long computeTimeoutNanos(long timeout, TimeUnit unit) {
         long taskTimeout = TimeUnit.MILLISECONDS.toNanos(getOwningTask().timeout());
         long futureTimeout = TimeUnit.NANOSECONDS.convert(timeout, unit);
         long actualTimeout;
         if (taskTimeout > 0 && futureTimeout > 0) {
            actualTimeout = Math.min(taskTimeout, futureTimeout);
         } else {
            actualTimeout = Math.max(taskTimeout, futureTimeout);
         }
         return actualTimeout;
      }

      protected ExecutionException wrapIntoExecutionException(Exception e){
         if (e instanceof ExecutionException) {
            return (ExecutionException) e;
         } else {
            return new ExecutionException(e);
         }
      }


      private V failoverExecution(final Exception cause, long timeout, TimeUnit unit)
               throws Exception {
         final List<Address> executionCandidates = executionCandidates(getOwningTask());
         FailoverContext fc = new FailoverContext() {
            @Override
            public <K> List<K> inputKeys() {
               return (List<K>) getInputKeys();
            }

            @Override
            public Address executionFailureLocation() {
               return getExecutionTarget();
            }

            @Override
            public List<Address> executionCandidates() {
               return executionCandidates;
            }

            @Override
            public Throwable cause() {
               return cause;
            }
         };

         Address target = getOwningTask().getTaskFailoverPolicy().failover(fc);
         DistributedTaskPart<V> part = createDistributedTaskPart(owningTask, distCommand,
                  getInputKeys(), target, failedOverCount);
         part.execute();
         return part.get(timeout, unit);
      }

      @Override
      public void notifyDone() {
         listenerLock.writeLock().lock();
         try {
            callCompleted = true;
            for (FutureListener<V> l : listeners) l.futureDone(this);
         } finally {
            listenerLock.writeLock().unlock();
         }
      }

      @Override
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

      V retrieveResult(Object response) throws Exception {
         V result = null;
         //this is application level Exception that was raised in execution
         //simply rethrow it (might be good candidate for failover)
         if (response instanceof Exception) {
            throw ((Exception) response);
         }
         //these two should never happen, mark them with IllegalStateException         
         if (response == null || !(response instanceof Map<?, ?>)) {
            throw new IllegalStateException("Invalid response received " + response);
         }         
         Map<Address, Response> mapResult = (Map<Address, Response>) response;
         if (mapResult.size() == 1) {
            for (Entry<Address, Response> e : mapResult.entrySet()) {
               Response value = e.getValue();
               if (value instanceof SuccessfulResponse) {
                  result = (V) ((SuccessfulResponse) value).getResponseValue();
               }
            }
         } else {
            //should never happen as we send DistributedTaskPart to one node for 
            //execution only, therefore we should get only one response
            throw new IllegalStateException("Invalid response " + response);
         }
         return result;
      }

      public void execute() {
         if (isLocalNodeExecutionTarget()) {
            invokeLocally();
         } else {
            if (trace) log.tracef("Sending %s to remote execution at node %s", f, getExecutionTarget());
            try {
               rpc.invokeRemotelyInFuture(Collections.singletonList(getExecutionTarget()), getCommand(),
                                          rpc.getRpcOptionsBuilder(ResponseMode.SYNCHRONOUS)
                                                .timeout(getOwningTask().timeout(), TimeUnit.MILLISECONDS).build(),
                                          (DistributedTaskPart<Object>) this);
            } catch (Throwable e) {
               log.remoteExecutionFailed(getExecutionTarget(), e);
            }
         }
      }

      protected void invokeLocally() {
         log.debugf("Sending %s to self", this);
         try {
            Callable<Object> call = new Callable<Object>() {

               @Override
               public Object call() throws Exception {
                  return doLocalInvoke();
               }

               private Object doLocalInvoke() {
                  Object result = null;
                  getCommand().init(cache);
                  DistributedTaskLifecycleService lifecycle = DistributedTaskLifecycleService.getInstance();
                  try {
                     // hook into lifecycle
                     lifecycle.onPreExecute(getCommand().getCallable(), cache);
                     cancellationService.register(Thread.currentThread(), getCommand().getUUID());
                     result = getCommand().perform(null);
                     return Collections.singletonMap(getAddress(),
                              SuccessfulResponse.create(result));
                  } catch (Throwable e) {
                     return e;
                  } finally {
                     // hook into lifecycle
                     lifecycle.onPostExecute(getCommand().getCallable());
                     cancellationService.unregister(getCommand().getUUID());
                     notifyDone();
                  }
               }
            };
            setNetworkFuture((Future<V>) localExecutorService.submit(call));
         } catch (Throwable e1) {
            log.localExecutionFailed(e1);
         }
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
         if (!(obj instanceof DistributedTaskPart)) {
            return false;
         }
         DistributedTaskPart other = (DistributedTaskPart) obj;
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
