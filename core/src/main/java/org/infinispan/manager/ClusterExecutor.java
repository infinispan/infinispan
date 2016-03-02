package org.infinispan.manager;

import org.infinispan.remoting.transport.Address;
import org.infinispan.util.SerializableFunction;
import org.infinispan.util.SerializableRunnable;
import org.infinispan.util.TriConsumer;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

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
    * failure.
    * @param command the command to execute
    */
   @Override
   void execute(Runnable command);

   /**
    * The same as {@link Executor#execute(Runnable)}, except the Runnable must also implement Serializable.
    * <p>
    * This method will be used automatically by lambdas, which prevents users from having to manually cast to
    * a Serializable lambda.
    * @param command the command to execute
    */
   void execute(SerializableRunnable command);

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
    * This method will be used automatically by lambdas, which prevents users from having to manually cast to
    * a Serializable lambda.
    * @param command the command to execute
    * @return a completable future that will signify the command is finished on all desired nodes when completed
    */
   CompletableFuture<Void> submit(SerializableRunnable command);

   /**
    * Submits the given command to the desired nodes and allows for handling of results as they return.  The user
    * provides a {@link TriConsumer} which will be called back each time for each desired node.  Note that these callbacks
    * can be called from different threads at the same time.  A completable future is returned to the caller used
    * for the sole purpose of being completed when all nodes have sent responses back.
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
   <V> CompletableFuture<Void> submitConsumer(SerializableFunction<? super EmbeddedCacheManager, ? extends V> callable,
                                              TriConsumer<? super Address, ? super V, ? super Throwable> triConsumer);

   /**
    * <p>The timeout parameter is not adhered to when executing the command locally and is only for remote nodes.
    * @param time
    * @param unit
    * @return
    */
   ClusterExecutor timeout(long time, TimeUnit unit);

   /**
    * Allows for filtering of address nodes dynamically per invocation.  The predicate is applied to each member in the
    * cluster at invocation to determine which targets to contact.  Note that this method overrides any previous
    * filtering that was done (ie. calling {@link ClusterExecutor#filterTargets(Collection)}).
    * @param predicate the dynamic predicate applied each time an invocation is done
    * @return this executor again with the predicate filter applied to determine which nodes are contacted
    */
   ClusterExecutor filterTargets(Predicate<? super Address> predicate);

   /**
    * Allows for filtering of address nodes by only allowing addresses in this collection from being contacted.
    * Note that this method overrides any previous filtering that was done (ie. calling
    * {@link ClusterExecutor#filterTargets(Predicate)}.
    * @param addresses which nodes the executor invocations should go to
    * @return this executor again which will only contact nodes whose address are in the given collection
    */
   ClusterExecutor filterTargets(Collection<Address> addresses);

   /**
    * Applies no filtering and will send any invocations to all current nodes.
    * @return this executor again with no filtering applied to target nodes
    */
   ClusterExecutor noFilter();
}
