package org.infinispan.manager;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.infinispan.configuration.global.TransportConfiguration;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.function.SerializableFunction;
import org.infinispan.util.function.SerializableRunnable;
import org.infinispan.util.function.TriConsumer;

/**
 * A cluster executor that can be used to invoke a given command across the cluster.  Note this executor is not
 * tied to any cache.
 * <p>
 * This executor also implements {@link Executor} so that it may be used with methods such as
 * {@link CompletableFuture#runAsync(Runnable, Executor)} or {@link CompletableFuture#supplyAsync(Supplier, Executor)}.
 * Unfortunately though these invocations do not have explicitly defined Serializable {@link Runnable} or
 * {@link Supplier} arguments and manual casting is required when using a lambda.
 * Something like the following:
 * {@code CompletableFuture.runAsync((Serializable && Runnable)() -> doSomething(), clusterExecutor)}.  Although note
 * that the {@link ClusterExecutor#submit(SerializableRunnable)} does this automatically for you.
 * <p>
 * Any method that returns a value should make sure the returned value is properly serializable or else it will
 * be replaced with a {@link org.infinispan.commons.marshall.NotSerializableException}
 * @author wburns
 * @since 8.2
 */
public interface ClusterExecutor extends Executor {
   /**
    * {@inheritDoc}
    * <p>
    * This command will be ran in the desired nodes, but no result is returned to notify the user of completion or
    * failure. This command will return immediately while the runnable is processed asynchronously.
    * @param command the command to execute
    */
   @Override
   default void execute(Runnable command) {
      submit(command);
   }

   /**
    * The same as {@link Executor#execute(Runnable)}, except the Runnable must also implement Serializable.
    * <p>
    * This method will be used automatically by lambdas, which prevents users from having to manually cast to
    * a Serializable lambda.
    * @param command the command to execute
    */
   default void execute(SerializableRunnable command) {
      execute((Runnable) command);
   }

   /**
    * Submits the runnable to the desired nodes and returns a CompletableFuture that will be completed when
    * all desired nodes complete the given command
    * <p>
    * If a node encounters an exception, the first one to respond with such an exception will set the responding
    * future to an exceptional state passing the given exception.
    * @param command the command to execute.
    * @return a completable future that will signify the command is finished on all desired nodes when completed
    */
   CompletableFuture<Void> submit(Runnable command);

   /**
    * The same as {@link ClusterExecutor#submit(Runnable)}, except the Runnable must also implement Serializable.
    * <p>
    *
    * <p>
    * This method will be used automatically by lambdas, which prevents users from having to manually cast to
    * a Serializable lambda.
    * @param command the command to execute
    * @return a completable future that will signify the command is finished on all desired nodes when completed
    */
   default CompletableFuture<Void> submit(SerializableRunnable command) {
      return submit((Runnable) command);
   }

   /**
    * Submits the given command to the desired nodes and allows for handling of results as they return.  The user
    * provides a {@link TriConsumer} which will be called back each time for each desired node.  Note that these callbacks
    * can be called from different threads at the same time.  A completable future is returned to the caller used
    * for the sole purpose of being completed when all nodes have sent responses back.
    * <p>
    * If this cluster executor is running in failover mode via {@link ClusterExecutor#singleNodeSubmission(int)} the
    * triConsumer will be called back each time a failure occurs as well. To satisfy ordering a retry is not resubmitted
    * until after the callback has completed.
    * <p>
    * Note the {@link TriConsumer} is only ran on the node where the task was submitted and thus doesn't need to be
    * serialized.
    * @param callable the task to execute
    * @param triConsumer the tri-consumer to be called back upon for each node's result
    * @param <V> the type of the task's result
    * @return a completable future that will be completed after all results have been processed
    */
   <V> CompletableFuture<Void> submitConsumer(Function<? super EmbeddedCacheManager, ? extends V> callable,
                                              TriConsumer<? super Address, ? super V, ? super Throwable> triConsumer);

   /**
    * The same as {@link ClusterExecutor#submitConsumer(Function, TriConsumer)}, except the Callable must also implement
    * Serializable.
    * <p>
    * This method will be used automatically by lambdas, which prevents users from having to manually cast to
    * a Serializable lambda.
    * @param callable the task to execute
    * @param triConsumer the tri-consumer to be called back upon for each node's result
    * @param <V> the type of the task's result
    * @return a completable future that will be completed after all results have been processed
    */
   default <V> CompletableFuture<Void> submitConsumer(SerializableFunction<? super EmbeddedCacheManager, ? extends V> callable,
                                              TriConsumer<? super Address, ? super V, ? super Throwable> triConsumer) {
      return submitConsumer((Function<? super EmbeddedCacheManager, ? extends V>) callable, triConsumer);
   }

   /**
    * Sets a duration after which a command will timeout. This will cause the command to return a
    * {@link org.infinispan.util.concurrent.TimeoutException} as the throwable.
    * <p>The timeout parameter is used for both local and remote nodes. There are no guarantees as to whether
    * the timed out command is interrupted.
    * @param time the duration for the timeout
    * @param unit what unit the duration is in
    * @return a cluster executor with a timeout applied for remote commands
    */
   ClusterExecutor timeout(long time, TimeUnit unit);

   /**
    * When a command is submitted it will only be submitted to one node of the available nodes, there is no strict
    * requirements as to which node is chosen and is implementation specific. Fail over is not used with the returned
    * executor, if you desire to use fail over you should invoke {@link ClusterExecutor#singleNodeSubmission(int)}
    * instead.
    * @return a cluster executor with commands submitted to a single node
    */
   ClusterExecutor singleNodeSubmission();

   /**
    * When a command is submitted it will only be submitted to one node of the available nodes, there is no strict
    * requirements as to which node is chosen and is implementation specific. However if a command were to fail either
    * by the command itself or via network issues then the command will fail over, that is that it will retried up to
    * the provided number of times using an available node until an exception is not met or the number of fail over
    * counts has been reached. If a {@link org.infinispan.util.concurrent.TimeoutException} is throwing, this will not
    * be retried as this is the same exception that is thrown when using
    * {@link ClusterExecutor#timeout(long, TimeUnit)}. Each time the
    * fail over occurs any available node is chosen, there is no requirement as to which can be chosen and is left up
    * to the implementation to decide.
    * @param failOverCount how many times this executor will attempt a failover
    * @return a cluster executor with fail over retries applied
    */
   ClusterExecutor singleNodeSubmission(int failOverCount);

   /**
    * When a command is submitted it will submit this command to all of the available nodes that pass the provided
    * filter.  Fail over is not supported with this configuration. This is the default submission method.
    * @return a cluster executor with commands submitted to all nodes
    */
   ClusterExecutor allNodeSubmission();

   /**
    * Allows for filtering of address nodes dynamically per invocation.  The predicate is applied to each member in the
    * cluster at invocation to determine which targets to contact.  Note that this method overrides any previous
    * filtering that was done (ie. calling {@link ClusterExecutor#filterTargets(Collection)}).
    * @param predicate the dynamic predicate applied each time an invocation is done
    * @return an executor with the predicate filter applied to determine which nodes are contacted
    */
   ClusterExecutor filterTargets(Predicate<? super Address> predicate);

   /**
    * Allows for filtering of address nodes by only allowing addresses that match the given execution policy to be used.
    * Note this method overrides any previous filtering that was done (ie. calling
    * {@link ClusterExecutor#filterTargets(Collection)}).
    * <p>
    * The execution policy is only used if the addresses are configured to be topology aware. That is that the
    * {@link TransportConfiguration#hasTopologyInfo()} method returns true.  If this is false this method will throw
    * an {@link IllegalStateException}.
    * @param policy the policy to determine which nodes can be used
    * @return an executor with the execution policy applied to determine which nodes are contacted
    * @throws IllegalStateException thrown if topology info isn't available
    */
   ClusterExecutor filterTargets(ClusterExecutionPolicy policy) throws IllegalStateException;

   /**
    * Allows for filtering of address nodes dynamically per invocation.  The predicate is applied to each member that
    * is part of the execution policy. Note that this method overrides any previous
    * filtering that was done (ie. calling {@link ClusterExecutor#filterTargets(Collection)}).
    * <p>
    * The execution policy is only used if the addresses are configured to be topology aware. That is that the
    * {@link TransportConfiguration#hasTopologyInfo()} method returns true.  If this is false this method will throw
    * an {@link IllegalStateException}.
    * @param policy the execution policy applied before predicate to allow only nodes in that group
    * @param predicate the dynamic predicate applied each time an invocation is done
    * @return an executor with the execution policy and predicate both applied to determine which nodes are contacted
    * @throws IllegalStateException thrown if topology info isn't available
    */
   ClusterExecutor filterTargets(ClusterExecutionPolicy policy, Predicate<? super Address> predicate)
         throws IllegalStateException;

   /**
    * Allows for filtering of address nodes by only allowing addresses in this collection from being contacted.
    * Note that this method overrides any previous filtering that was done (ie. calling
    * {@link ClusterExecutor#filterTargets(Predicate)}.
    * @param addresses which nodes the executor invocations should go to
    * @return an executor which will only contact nodes whose address are in the given collection
    */
   ClusterExecutor filterTargets(Collection<Address> addresses);

   /**
    * Applies no filtering and will send any invocations to any/all current nodes.
    * @return an executor with no filtering applied to target nodes
    */
   ClusterExecutor noFilter();
}
