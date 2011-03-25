package org.infinispan.distexec;

import java.io.Externalizable;
import java.io.NotSerializableException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.infinispan.Cache;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.read.DistributedExecuteCommand;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.concurrent.NotifyingFuture;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Infinispan's implementation of an {@link ExecutorService}. This ExecutorService provides methods
 * to submit tasks for an execution on a cluster of Infinispan nodes.
 * <p>
 * 
 * 
 * Note that due to potential task migration to another nodes every {@link Callable},
 * {@link Runnable} and/or {@link DistributedCallable} submitted must be either {@link Serializable}
 * or {@link Externalizable}. Also the value returned from a callable must be {@link Serializable}
 * or {@link Externalizable}. Unfortunately if the value returned is not serializable then a
 * {@link NotSerializableException} will be thrown.
 * 
 */
public class DefaultExecutorService extends AbstractExecutorService implements
         DistributedExecutorService {

   private static final Log log = LogFactory.getLog(DefaultExecutorService.class);
   protected final AtomicBoolean isShutdown = new AtomicBoolean(false);
   protected final Cache cache;
   protected final RpcManager rpc;
   protected final InterceptorChain invoker;
   protected final CommandsFactory factory;

   public DefaultExecutorService(Cache cache) {
      super();
      this.cache = cache;
      this.rpc = cache.getAdvancedCache().getRpcManager();
      ComponentRegistry registry = cache.getAdvancedCache().getComponentRegistry();
      this.invoker = registry.getComponent(InterceptorChain.class);
      this.factory = registry.getComponent(CommandsFactory.class);
   }

   @Override
   public <T> NotifyingFuture<T> submit(Runnable task, T result) {
      // This cast is okay cause we control creation of the task
      return (NotifyingFuture<T>) super.submit(task, result);
   }

   @Override
   public <T> NotifyingFuture<T> submit(Callable<T> task) {
      // This cast is okay cause we control creation of the task
      return (NotifyingFuture<T>) super.submit(task);
   }

   @Override
   public void shutdown() {
      realShutdown(false);
   }

   @SuppressWarnings("unchecked")
   private List<Runnable> realShutdown(boolean interrupt) {
      isShutdown.set(true);
      // TODO cancel all tasks
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
      long nanoTimeWait = unit.toNanos(timeout);
      // TODO wait for all tasks to finish
      return true;
   }

   public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException,
            ExecutionException {
      try {
         return doInvokeAny(tasks, false, 0);
      } catch (TimeoutException cannotHappen) {
         assert false;
         return null;
      }
   }

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
         DistributedRunnableFuture<Object> cmd = null;
         if (command instanceof DistributedRunnableFuture<?>) {
            cmd = (DistributedRunnableFuture<Object>) command;
         } else if (command instanceof Serializable) {
            cmd = (DistributedRunnableFuture<Object>) newTaskFor(command, null);
         } else {
            throw new IllegalArgumentException("Runnable command is not Serializable  " + command);
         }         
         sendForRemoteExecution(randomClusterMemberOtherThanSelf(), cmd);
      } else {
         throw new RejectedExecutionException();
      }
   }

   protected static final class RunnableAdapter<T> implements Callable<T>, Serializable {

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

      public T call() {
         task.run();
         return result;
      }
   }

   @Override
   protected <T> RunnableFuture<T> newTaskFor(Runnable runnable, T value) {
      DistributedExecuteCommand<T> executeCommand = factory.buildDistributedExecuteCommand(
               new RunnableAdapter<T>(runnable, value), rpc.getAddress(), null);
      DistributedRunnableFuture<T> future = new DistributedRunnableFuture<T>(executeCommand);
      return future;
   }

   @Override
   protected <T> RunnableFuture<T> newTaskFor(Callable<T> callable) {
      DistributedExecuteCommand<T> executeCommand = factory.buildDistributedExecuteCommand(
               callable, rpc.getAddress(), null);
      DistributedRunnableFuture<T> future = new DistributedRunnableFuture<T>(executeCommand);
      return future;
   }

   @Override
   public <T, K> Future<T> submit(Callable<T> task, K... input) {
      Map<Address, List<K>> nodesKeysMap = mapKeysToNodes(input);
      Collection<Object> keys = (Collection<Object>) Arrays.asList(input);
      DistributedExecuteCommand<T> c = factory.buildDistributedExecuteCommand(task,
               rpc.getAddress(), keys);
      DistributedRunnableFuture<T> f = new DistributedRunnableFuture<T>(c);
      ArrayList<Address> nodes = new ArrayList<Address>(nodesKeysMap.keySet());
      boolean invokeOnSelf = (nodes.size() == 1 && nodes.get(0).equals(rpc.getAddress()));
      if (invokeOnSelf) {
         invokeLocally(f);
      } else {
         sendForRemoteExecution(randomClusterMemberExcludingSelf(nodes), f);
      }
      return f;
   }

   @Override
   public <T> List<Future<T>> submitEverywhere(Callable<T> task) {
      List<Future<T>> futures = new ArrayList<Future<T>>();
      List<Address> members = rpc.getTransport().getMembers();
      List<Address> membersCopy = new ArrayList<Address>(members);
      membersCopy.remove(rpc.getAddress());
      for (Address address : membersCopy) {
         DistributedExecuteCommand<T> c = factory.buildDistributedExecuteCommand(task,
                  rpc.getAddress(), null);
         DistributedRunnableFuture<T> f = new DistributedRunnableFuture<T>(c);
         futures.add(f);
         sendForRemoteExecution(address, f);
      }
      DistributedExecuteCommand<T> c = factory.buildDistributedExecuteCommand(task,
               rpc.getAddress(), null);
      DistributedRunnableFuture<T> f = new DistributedRunnableFuture<T>(c);
      futures.add(f);
      invokeLocally(f);      
      return futures;
   }

   @Override
   public <T, K> List<Future<T>> submitEverywhere(Callable<T> task, K... input) {
      List<Future<T>> futures = new ArrayList<Future<T>>();
      Map<Address, List<K>> nodesKeysMap = mapKeysToNodes(input);
      DistributedRunnableFuture<T> selfFuture = null;
      for (Entry<Address, List<K>> e : nodesKeysMap.entrySet()) {
         Address target = e.getKey();
         DistributedExecuteCommand<T> c = factory.buildDistributedExecuteCommand(task,
                  rpc.getAddress(), (Collection<Object>) e.getValue());
         DistributedRunnableFuture<T> f = new DistributedRunnableFuture<T>(c);
         futures.add(f);
         if (target.equals(rpc.getAddress())) {
            invokeLocally(f);
         } else {
            sendForRemoteExecution(target, f);
         }
      }
      return futures;
   }

   protected <T> void sendForRemoteExecution(Address address, DistributedRunnableFuture<T> f) {
      log.debug("Sending %s to remote execution at node %s", f, address);
      try {
         rpc.invokeRemotelyInFuture(Collections.singletonList(address), f.getCommand(), f);
      } catch (Throwable e) {
         log.warn("Falied remote execution on node " + address, e);
      }
   }

   protected <T> void invokeLocally(final DistributedRunnableFuture<T> future) {
      log.debug("Sending %s to self", future);
      try {
         Callable<T> call = new Callable<T>() {
            @Override
            public T call() throws Exception {
               Object result = null;
               future.getCommand().init(cache);
               try {
                  result = future.getCommand().perform(null);
               } catch (Throwable e) {
                  result = e;
               } finally {
                  future.notifyDone();
               }
               return (T) result;
            }
         };
         final FutureTask<T> task = new FutureTask<T>(call);
         future.setNetworkFuture(task);
         Thread t = new Thread() {
            @Override
            public void run() {
               task.run();
            }
         };
         t.start();
      } catch (Throwable e1) {
         log.warn("Falied local execution ", e1);
      }
   }

   protected <K> Map<Address, List<K>> mapKeysToNodes(K... input) {
      DistributionManager dm = cache.getAdvancedCache().getDistributionManager();
      Map<Address, List<K>> addressToKey = new HashMap<Address, List<K>>();
      for (K key : input) {
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

   protected List<Address> randomClusterMembers(int numNeeded) {
      List<Address> members = new ArrayList<Address>(rpc.getTransport().getMembers());
      return randomClusterMembers(members, numNeeded);
   }
   
   protected Address randomClusterMemberExcludingSelf(List<Address> members) {
      List<Address> list = randomClusterMembers(members,1);
      return list.get(0);
   }
   
   protected Address randomClusterMemberOtherThanSelf() {
     List<Address> l = randomClusterMembers(1);
     return l.get(0);
   }

   protected List<Address> randomClusterMembers(List<Address> members, int numNeeded) {
      List<Address> chosen = new ArrayList<Address>();
      members.remove(rpc.getAddress());
      if (members.size() < numNeeded) {
         log.warn("Can not select %s random members for %s", numNeeded, members);
         numNeeded = members.size();
      }
      Random r = new Random();
      while (members != null && !members.isEmpty() && numNeeded >= chosen.size()) {
         int count = members.size();
         Address address = members.remove(r.nextInt(count));
         chosen.add(address);
      }
      return chosen;
   }
}
