package org.infinispan.distexec;

import java.io.Externalizable;
import java.io.NotSerializableException;
import java.io.Serializable;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import org.infinispan.remoting.transport.Address;

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
 * Note that due to potential task migration to other nodes every {@link Callable},
 * {@link Runnable} and/or {@link DistributedCallable} submitted must be either {@link Serializable}
 * or {@link Externalizable}. Also the value returned from a callable must be {@link Serializable}
 * or {@link Externalizable}. Unfortunately if the value returned is not serializable then a
 * {@link NotSerializableException} will be thrown.
 * <p>
 *
 * All {@link CompletableFuture} returned to the caller may be cancelled, however if interruption is desired,
 * (ie. <b>mayInterruptIfRunning == true</b>), it will only be performed if done on the original
 * <b>CompletableFuture</b> returned via one of the various submit methods.  Any chained
 * futures from the original will only attempt to cancel the task result.  If interruption is needed and this task
 * was found to be operating on a remote node it will send a cancellation command to the remote node in an attempt to
 * stop it early using standard java {@link Thread#interrupt()} on the thread processing that task.
 *
 * @see DefaultExecutorService
 * @see DistributedCallable
 *
 * @author Manik Surtani
 * @author Vladimir Blagojevic
 *
 * @since 5.0
 * @deprecated since 9.1 Please use {@link org.infinispan.manager.ClusterExecutor} or {@link org.infinispan.CacheStream} instead.
 */
public interface DistributedExecutorService extends ExecutorService {

   /**
    *  Submits the given Callable task for execution on the specified target Infinispan node.
    * <p>
    *
    * @param <T>
    * @param target address of Infinispan node selected for execution of the task
    * @param task a task to execute on selected Infinispan node
    * @return a Future representing pending completion of the task
    */
   <T> CompletableFuture<T> submit(Address target, Callable<T> task);

   /**
    *  Submits the given DistributedTask for execution on the specified target Infinispan node.
    * <p>
    *
    * @param <T>
    * @param target address of Infinispan node selected for execution of the task
    * @param task a task to execute on selected Infinispan node
    * @return a Future representing pending completion of the task
    */
   <T> CompletableFuture<T> submit(Address target, DistributedTask<T> task);

   /**
    * Submits the given Callable task for execution on a single Infinispan node.
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
   <T, K> CompletableFuture<T> submit(Callable<T> task, K... input);

   /**
    * Submits the given DistributedTask for execution on a single Infinispan node.
    * <p>
    *
    * Execution environment will chose an arbitrary node N hosting some or all of the keys specified
    * as input. If all keys are not available locally at node N they will be retrieved from the
    * cluster.
    *
    * @param task
    *           a DistributedTask to execute across Infinispan cluster
    * @param input
    *           input keys for this task, effective if and only if task's callable is instance of
    *           {@link DistributedCallable}
    * @return a Future representing pending completion of the task
    */
   <T, K> CompletableFuture<T> submit(DistributedTask<T> task, K... input);

   /**
    * Submits the given Callable task for execution on all available Infinispan nodes.
    *
    * @param task a task to execute across Infinispan cluster
    * @return a list of Futures, one future per Infinispan cluster node where task was executed
    */
   <T> List<CompletableFuture<T>> submitEverywhere(Callable<T> task);

   /**
    * Submits the given DistributedTask for execution on all available Infinispan nodes.
    *
    * @param task a task to execute across Infinispan cluster
    * @return a list of Futures, one future per Infinispan cluster node where task was executed
    */
   <T> List<CompletableFuture<T>> submitEverywhere(DistributedTask<T> task);

   /**
    * Submits the given Callable task for execution on all available Infinispan nodes using input
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
   <T, K> List<CompletableFuture<T>> submitEverywhere(Callable<T> task, K... input);

   /**
    * Submits the given DistributedTask for execution on all available Infinispan nodes using input
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
   <T, K> List<CompletableFuture<T>> submitEverywhere(DistributedTask<T> task, K... input);

   /**
    * Returns DistributedTaskBuilder for this DistributedExecutorService and a given Callable. As it
    * name implies clients can use DistributedTaskBuilder to create DistributedTask instances.
    *
    * @param <T>
    * @param callable the execution unit of DistributedTask
    * @return DistributedTaskBuilder to create {@link DistributedTask}
    */
   <T> DistributedTaskBuilder<T> createDistributedTaskBuilder(Callable<T> callable);

}
