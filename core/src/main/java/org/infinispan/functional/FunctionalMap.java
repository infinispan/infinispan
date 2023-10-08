package org.infinispan.functional;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import org.infinispan.Cache;
import org.infinispan.commons.util.Experimental;
import org.infinispan.functional.EntryView.ReadEntryView;
import org.infinispan.functional.EntryView.ReadWriteEntryView;
import org.infinispan.functional.EntryView.WriteEntryView;
import org.infinispan.functional.Listeners.ReadWriteListeners;
import org.infinispan.functional.Listeners.WriteListeners;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.marshall.core.MarshallableFunctions;
import org.infinispan.util.function.SerializableBiConsumer;
import org.infinispan.util.function.SerializableBiFunction;
import org.infinispan.util.function.SerializableConsumer;
import org.infinispan.util.function.SerializableFunction;

/**
 * Top level functional map interface offering common functionality for the
 * read-only, read-write, and write-only operations that can be run against a
 * functional map asynchronously.
 *
 * <p>Lambdas passed in as parameters to functional map methods define the
 * type of operation that is executed, but since lambdas are transparent to
 * the internal logic, it was decided to separate the API into three types
 * of operation: read-only, write-only, and read-write. This separation helps
 * the user understand the group of functions and their possibilities.
 *
 * <p>This conscious decision to separate read-only, write-only and
 * read-write interfaces helps type safety. So, if a user gets a read-only
 * map, it can't write to it by mistake since no such APIs are exposed.
 * The same happens with write-only maps, the user can only write and cannot
 * make the mistake of reading from the entry view because read operations
 * are not exposed.
 *
 * <p>Lambdas passed in to read-write and write-only operations, when
 * running in a cluster, must be marshallable. Marshallable lambdas for some of
 * the most popular lambda functions used by {@link ConcurrentMap} are
 * available via the {@link MarshallableFunctions} helper class.
 *
 * <p>Being an asynchronous API, all methods that return a single result,
 * return a {@link CompletableFuture} which wraps the result. To avoid
 * blocking, it offers the possibility to receive callbacks when the
 * {@link CompletableFuture} has completed, or it can be chained or composes
 * with other {@link CompletableFuture} instances.
 *
 * <p>For those operations that return multiple results, the API returns
 * instances of a {@link Traversable} interface which offers a lazy pull­style
 * API for working with multiple results. Although push­style interfaces for
 * handling multiple results, such as RxJava, are fully asynchronous, they're
 * harder to use from a user’s perspective. {@link Traversable},​ being a lazy
 * pull­style API, can still be asynchronous underneath since the user can
 * decide to work on the {@link Traversable} at a later stage, and the
 * implementation itself can decide when to compute those results.
 *
 * @since 8.0
 */
@Experimental
public interface FunctionalMap<K, V> extends AutoCloseable {

   /**
    * Tweak functional map executions providing {@link Param} instances.
    */
   FunctionalMap<K, V> withParams(Param<?>... ps);

   /**
    * Functional map's name.
    */
   String getName();

   /**
    * Functional map's status.
    */
   ComponentStatus getStatus();

   /**
    * Tells if the underlying cache is using encoding or not
    *
    * @return true if the underlying cache is encoded
    */
   default boolean isEncoded() {
      return false;
   }

   Cache<K, V> cache();

   /**
    * Exposes read-only operations that can be executed against the functional map.
    * The information that can be read per entry in the functional map is
    * exposed by {@link ReadEntryView}.
    *
    * <p>Read-only operations have the advantage that no locks are acquired
    * for the duration of the operation and so it makes sense to have them
    * a top-level interface dedicated to them.
    *
    * <p>Browsing methods that provide a read-only view of the cached data
    * are available via {@link #keys()} and {@link #entries()}.
    * Having {@link #keys()} makes sense since that way keys can be traversed
    * without having to bring values. Having {@link #entries()} makes sense
    * since it allows traversing both keys, values and any meta parameters
    * associated with them, but this is no extra cost to exposing just values
    * since keys are the main index and hence will always be available.
    * Hence, adding a method to only browse values offers nothing extra to
    * the API.
    *
    * @since 8.0
    */
   @Experimental
   interface ReadOnlyMap<K, V> extends FunctionalMap<K, V> {
      /**
       * Tweak read-only functional map executions providing {@link Param} instances.
       */
      ReadOnlyMap<K, V> withParams(Param<?>... ps);

      /**
       * Evaluate a read-only function on the value associated with the key
       * and return a {@link CompletableFuture} with the return type of the function.
       * If the user is not sure if the key is present, {@link ReadEntryView#find()}
       * can be used to find out for sure. Typically, function implementations
       * would return value or {@link MetaParam} information from the cache
       * entry in the functional map.
       *
       * <p>By returning {@link CompletableFuture} instead of the function's
       * return type directly, the method hints at the possibility that to
       * execute the function might require to go remote to retrieve data in
       * persistent store or another clustered node.
       *
       * <p>This method can be used to implement read-only single-key based
       * operations in {@link ConcurrentMap} such as:
       *
       * <ul>
       *    <li>{@link ConcurrentMap#get(Object)}</li>
       *    <li>{@link ConcurrentMap#containsKey(Object)}</li>
       * </ul>
       *
       * <p>The function must not mutate neither the key returned through
       * {@link ReadEntryView#key()} nor the internally stored value provided
       * through {@link ReadEntryView#get()} or {@link ReadEntryView#find()}.
       *
       * @param key the key associated with the {@link ReadEntryView} to be
       *            passed to the function.
       * @param f function that takes a {@link ReadEntryView} associated with
       *          the key, and returns a value.
       * @param <R> function return type
       * @return a {@link CompletableFuture} which will be completed with the
       *         returned value from the function
       */
      <R> CompletableFuture<R> eval(K key, Function<ReadEntryView<K, V>, R> f);

      /**
       * Same as {@link #eval(Object, Function)} except that the function must also
       * implement <code>Serializable</code>
       * <p>
       * The compiler will pick this overload for lambda parameters, making them <code>Serializable</code>
       */
      default <R> CompletableFuture<R> eval(K key, SerializableFunction<ReadEntryView<K, V>, R> f) {
         return eval(key, (Function<ReadEntryView<K, V>, R>) f);
      }

      /**
       * Evaluate a read-only function on a key and potential value associated in
       * the functional map, for each of the keys in the set passed in, and
       * returns an {@link Traversable} to work on each computed function's result.
       *
       * <p>The function passed in will be executed for as many keys
       * present in keys collection set. Similar to {@link #eval(Object, Function)},
       * if the user is not sure whether a particular key is present,
       * {@link ReadEntryView#find()} can be used to find out for sure.
       *
       * DESIGN RATIONALE:
       * <ul>
       *    <li>It makes sense to expose global operation like this instead of
       *    forcing users to iterate over the keys to lookup and call get
       *    individually since Infinispan can do things more efficiently.
       *    </li>
       * </ul>
       *
       * <p>The function must not mutate neither the key returned through
       * {@link ReadEntryView#key()} nor the internally stored value provided
       * through {@link ReadEntryView#get()} or {@link ReadEntryView#find()}.
       *
       * @param keys the keys associated with each of the {@link ReadEntryView}
       *             passed in the function callbacks
       * @param f function that takes a {@link ReadEntryView} associated with
       *          the key, and returns a value. It'll be invoked once for each key
       *          passed in
       * @param <R> function return type
       * @return a sequential {@link Traversable} that can be navigated to
       *         retrieve each function return value
       */
      <R> Traversable<R> evalMany(Set<? extends K> keys, Function<ReadEntryView<K, V>, R> f);

      /**
       * Same as {@link #evalMany(Set, Function)} except that the function must also
       * implement <code>Serializable</code>
       * <p>
       * The compiler will pick this overload for lambda parameters, making them <code>Serializable</code>
       */
      default <R> Traversable<R> evalMany(Set<? extends K> keys, SerializableFunction<ReadEntryView<K, V>, R> f) {
         return evalMany(keys, (Function<ReadEntryView<K, V>, R>) f);
      }


      /**
       * Provides a {@link Traversable} that allows clients to navigate all cached keys.
       *
       * <p>This method can be used to implement operations such as:
       * <ul>
       *    <li>{@link ConcurrentMap#size()}</li>
       *    <li>{@link ConcurrentMap#keySet()}</li>
       *    <li>{@link ConcurrentMap#isEmpty()}</li>
       * </ul>
       *
       * @return a sequential {@link Traversable} to navigate each cached key
       */
      Traversable<K> keys();

      /**
       * Provides a {@link Traversable} that allows clients to navigate all cached entries.
       *
       * <p>This method can be used to implement operations such as:
       * <ul>
       *    <li>{@link ConcurrentMap#containsValue(Object)}</li>
       *    <li>{@link ConcurrentMap#values()}</li>
       *    <li>{@link ConcurrentMap#entrySet()}</li>
       * </ul>
       *
       * @return a sequential {@link Traversable} to navigate each cached entry
       */
      Traversable<ReadEntryView<K, V>> entries();
   }

   /**
    * Exposes write-only operations that can be executed against the functional map.
    * The write operations that can be applied per entry are exposed by
    * {@link WriteEntryView}.
    *
    * <p>Write-only operations require locks to be acquired but crucially
    * they do not require reading previous value or metadata parameter
    * information associated with the cached entry, which sometimes can be
    * expensive since they involve talking to a remote node in the cluster
    * or the persistence layer So, exposing write-only operations makes it
    * easy to take advantage of this important optimisation.
    *
    * <p>Method parameters for write-only operations, including lambdas,
    * must be marshallable when running in a cluster.
    *
    * @since 8.0
    */
   @Experimental
   interface WriteOnlyMap<K, V> extends FunctionalMap<K, V> {
      /**
       * Tweak write-only functional map executions providing {@link Param} instances.
       */
      WriteOnlyMap<K, V> withParams(Param<?>... ps);

      /**
       * Evaluate a write-only {@link BiConsumer} operation, with an argument
       * passed in and a {@link WriteEntryView} of the value associated with
       * the key, and return a {@link CompletableFuture} which will be
       * completed when the operation completes.
       *
       * <p>Since this is a write-only operation, no entry attributes can be
       * queried, hence the only reasonable thing can be returned is Void.
       *
       * <p>This method can be used to implement single-key write-only operations
       * which do not need to query the previous value.
       *
       * <p>This operation is very similar to {@link #eval(Object, Consumer)}
       * and in fact, the functionality provided by this function could indeed
       * be implemented with {@link #eval(Object, Consumer)}, but there's a
       * crucial difference. If you want to store a value and reference the
       * value to be stored from the passed in operation,
       * {@link #eval(Object, Consumer)} needs to capture that value.
       * Capturing means that each time the operation is called, a new lambda
       * needs to be instantiated. By offering a {@link BiConsumer} that
       * takes user provided value as first parameter, the operation does not
       * capture any external objects when implementing simple operations,
       * and hence, the {@link BiConsumer} could be cached and reused each
       * time it's invoked.
       *
       * <p>Note that when {@link org.infinispan.commons.dataconversion.Encoder encoders}
       * are in place despite the argument type and value type don't have to match
       * the argument will use value encoding.
       *
       * @param key the key associated with the {@link WriteEntryView} to be
       *            passed to the operation
       * @param argument argument passed in as first parameter to the
       *              {@link BiConsumer} operation.
       * @param f operation that takes a user defined value, and a
       *          {@link WriteEntryView} associated with the key, and writes
       *          to the {@link WriteEntryView} passed in without returning anything
       * @return a {@link CompletableFuture} which will be completed when the
       *         operation completes
       */
      <T> CompletableFuture<Void> eval(K key, T argument, BiConsumer<T, WriteEntryView<K, V>> f);

      /**
       * Same as {@link #eval(Object, Object, BiConsumer)} except that the function must also
       * implement <code>Serializable</code>
       * <p>
       * The compiler will pick this overload for lambda parameters, making them <code>Serializable</code>
       */
      default <T> CompletableFuture<Void> eval(K key, T argument, SerializableBiConsumer<T, WriteEntryView<K, V>> f) {
         return eval(key, argument, (BiConsumer<T, WriteEntryView<K, V>>) f);
      }

      /**
       * Evaluate a write-only {@link Consumer} operation with a
       * {@link WriteEntryView} of the value associated with the key,
       * and return a {@link CompletableFuture} which will be
       * completed with the object returned by the operation.
       *
       * <p>Since this is a write-only operation, no entry attributes can be
       * queried, hence the only reasonable thing can be returned is Void.
       *
       * <p>This operation can be used to either remove a cached entry,
       * or to write a constant value along with optional metadata parameters.
       *
       * @param key the key associated with the {@link WriteEntryView} to be
       *            passed to the operation
       * @param f operation that takes a {@link WriteEntryView} associated with
       *          the key and writes to the it without returning anything
       * @return a {@link CompletableFuture} which will be completed when the
       *         operation completes
       */
      CompletableFuture<Void> eval(K key, Consumer<WriteEntryView<K, V>> f);

      /**
       * Same as {@link #eval(Object, Consumer)} except that the function must also
       * implement <code>Serializable</code>
       * <p>
       * The compiler will pick this overload for lambda parameters, making them <code>Serializable</code>
       */
      default CompletableFuture<Void> eval(K key, SerializableConsumer<WriteEntryView<K, V>> f) {
         return eval(key, (Consumer<WriteEntryView<K, V>>) f);
      }

      /**
       * Evaluate a write-only {@link BiConsumer} operation, with an argument
       * passed in and a {@link WriteEntryView} of the value associated with
       * the key, for each of the keys in the set passed in, and returns a
       * {@link CompletableFuture} that will be completed when the write-only
       * operation has been executed against all the entries.
       *
       * <p>These kind of operations are preferred to traditional end user
       * iterations because the internal logic can often iterate more
       * efficiently since it knows more about the system.
       *
       * <p>Since this is a write-only operation, no entry attributes can be
       * queried, hence the only reasonable thing can be returned is Void.
       *
       * <p>Note that when {@link org.infinispan.commons.dataconversion.Encoder encoders}
       * are in place despite the argument type and value type don't have to match
       * the argument will use value encoding.
       *
       * @param arguments the key/value pairs associated with each of the
       *             {@link WriteEntryView} passed in the function callbacks
       * @param f operation that consumes a value associated with a key in the
       *          entries collection and the {@link WriteEntryView} associated
       *          with that key in the cache
       * @return a {@link CompletableFuture} which will be completed when
       *         the {@link BiConsumer} operation  has been executed against
       *         all entries
       */
      <T> CompletableFuture<Void> evalMany(Map<? extends K, ? extends T> arguments, BiConsumer<T, WriteEntryView<K, V>> f);

      /**
       * Same as {@link #evalMany(Map, BiConsumer)} except that the function must also
       * implement <code>Serializable</code>
       * <p>
       * The compiler will pick this overload for lambda parameters, making them <code>Serializable</code>
       */
      default <T> CompletableFuture<Void> evalMany(Map<? extends K, ? extends T> arguments, SerializableBiConsumer<T, WriteEntryView<K, V>> f) {
         return evalMany(arguments, (BiConsumer<T, WriteEntryView<K, V>>) f);
      }

      /**
       * Evaluate a write-only {@link Consumer} operation with the
       * {@link WriteEntryView} of the value associated with the key, for each
       * of the keys in the set passed in, and returns a
       * {@link CompletableFuture} that will be completed when the write-only
       * operation has been executed against all the entries.
       *
       * <p>These kind of operations are preferred to traditional end user
       * iterations because the internal logic can often iterate more
       * efficiently since it knows more about the system.
       *
       * <p>Since this is a write-only operation, no entry attributes can be
       * queried, hence the only reasonable thing can be returned is Void.
       *
       * @param keys the keys associated with each of the {@link WriteEntryView}
       *             passed in the function callbacks
       * @param f operation that the {@link WriteEntryView} associated with
       *          one of the keys passed in
       * @return a {@link CompletableFuture} which will be completed when
       *         the {@link Consumer} operation has been executed against all
       *         entries
       */
      CompletableFuture<Void> evalMany(Set<? extends K> keys, Consumer<WriteEntryView<K, V>> f);

      /**
       * Same as {@link #evalMany(Set, Consumer)} except that the function must also
       * implement <code>Serializable</code>
       * <p>
       * The compiler will pick this overload for lambda parameters, making them <code>Serializable</code>
       */
      default CompletableFuture<Void> evalMany(Set<? extends K> keys, SerializableConsumer<WriteEntryView<K, V>> f) {
         return evalMany(keys, (Consumer<WriteEntryView<K, V>>) f);
      }

      /**
       * Evaluate a write-only {@link Consumer} operation with the
       * {@link WriteEntryView} of the value associated with the key, for all
       * existing keys in functional map, and returns a {@link CompletableFuture}
       * that will be completed when the write-only operation has been executed
       * against all the entries.
       *
       * @param f operation that the {@link WriteEntryView} associated with
       *          one of the keys passed in
       * @return a {@link CompletableFuture} which will be completed when
       *         the {@link Consumer} operation has been executed against all
       *         entries
       */
      CompletableFuture<Void> evalAll(Consumer<WriteEntryView<K, V>> f);

      /**
       * Same as {@link #evalAll(Consumer)} except that the function must also
       * implement <code>Serializable</code>
       * <p>
       * The compiler will pick this overload for lambda parameters, making them <code>Serializable</code>
       */
      default CompletableFuture<Void> evalAll(SerializableConsumer<WriteEntryView<K, V>> f) {
         return evalAll((Consumer<WriteEntryView<K, V>>) f);
      }

      /**
       * Truncate the contents of the cache, returning a {@link CompletableFuture}
       * that will be completed when the truncate process completes.
       *
       * This method can be used to implement:
       *
       * <ul>
       *    <li>{@link ConcurrentMap#clear()}</li>
       * </ul>
       *
       * @return a {@link CompletableFuture} that completes when the truncat
       *         has finished
       */
      CompletableFuture<Void> truncate();

      /**
       * Allows to write-only listeners to be registered.
       */
      WriteListeners<K, V> listeners();
   }

   /**
    * Exposes read-write operations that can be executed against the functional map.
    * The read-write operations that can be applied per entry are exposed by
    * {@link ReadWriteEntryView}.
    *
    * <p>Read-write operations offer the possibility of writing values or
    * metadata parameters, and returning previously stored information.
    * Read-write operations are also crucial for implementing conditional,
    * compare-and-swap (CAS) like operations.
    *
    * <p>Locks are acquired before executing the read-write lambda.
    *
    * <p>Method parameters for read-write operations, including lambdas,
    * must be marshallable when running in a cluster.
    *
    * @since 8.0
    */
   @Experimental
   interface ReadWriteMap<K, V> extends FunctionalMap<K, V> {
      /**
       * Tweak read-write functional map executions providing {@link Param} instances.
       */
      ReadWriteMap<K, V> withParams(Param<?>... ps);

      /**
       * Evaluate a read-write function on the value and metadata associated
       * with the key and return a {@link CompletableFuture} with the return
       * type of the function. If the user is not sure if the key is present,
       * {@link ReadWriteEntryView#find()} can be used to find out for sure.
       *
       * This method can be used to implement single-key read-write operations
       * in {@link ConcurrentMap} that do not depend on value information given
       * by the user such as:
       *
       * <ul>
       *    <li>{@link ConcurrentMap#remove(Object)}</li>
       * </ul>
       *
       * <p>The function must not mutate neither the key returned through
       * {@link ReadEntryView#key()} nor the internally stored value provided
       * through {@link ReadEntryView#get()} or {@link ReadEntryView#find()}.
       *
       * @param key the key associated with the {@link ReadWriteEntryView} to be
       *            passed to the function.
       * @param f function that takes a {@link ReadWriteEntryView} associated with
       *          the key, and returns a value.
       * @param <R> function return type
       * @return a {@link CompletableFuture} which will be completed with the
       *         returned value from the function
       */
      <R> CompletableFuture<R> eval(K key, Function<ReadWriteEntryView<K, V>, R> f);

      /**
       * Same as {@link #eval(Object, Function)}  except that the function must also
       * implement <code>Serializable</code>
       * <p>
       * The compiler will pick this overload for lambda parameters, making them <code>Serializable</code>
       */
      default <R> CompletableFuture<R> eval(K key, SerializableFunction<ReadWriteEntryView<K, V>, R> f) {
         return eval(key, (Function<ReadWriteEntryView<K, V>, R>) f);
      }

      /**
       * Evaluate a read-write function, with an argument passed in and a
       * {@link WriteEntryView} of the value associated with the key, and
       * return a {@link CompletableFuture} which will be completed with the
       * returned value by the function.
       *
       * <p>This method provides the the capability to both update the value and
       * metadata associated with that key, and return previous value or metadata.
       *
       * <p>This method can be used to implement the vast majority of single-key
       * read-write operations in {@link ConcurrentMap} such as:
       *
       * <ul>
       * <li>{@link ConcurrentMap#put(Object, Object)}</li>
       * <li>{@link ConcurrentMap#putIfAbsent(Object, Object)}</li>
       * <li>{@link ConcurrentMap#replace(Object, Object)}</li>
       * <li>{@link ConcurrentMap#replace(Object, Object, Object)}</li>
       * <li>{@link ConcurrentMap#remove(Object, Object)}</li>
       * </ul>
       *
       * <p> The functionality provided by this function could indeed be
       * implemented with {@link #eval(Object, Function)}, but there's a
       * crucial difference. If you want to store a value and reference the
       * value to be stored from the passed in operation,
       * {@link #eval(Object, Function)} needs to capture that value.
       * Capturing means that each time the operation is called, a new lambda
       * needs to be instantiated. By offering a {@link BiFunction} that
       * takes user provided value as first parameter, the operation does
       * not capture any external objects when implementing
       * simple operations, and hence, the {@link BiFunction} could be cached
       * and reused each time it's invoked.
       *
       * <p>Note that when {@link org.infinispan.commons.dataconversion.Encoder encoders}
       * are in place despite the argument type and value type don't have to match
       * the argument will use value encoding.
       *
       * <p>The function must not mutate neither the key returned through
       * {@link ReadEntryView#key()} nor the internally stored value provided
       * through {@link ReadEntryView#get()} or {@link ReadEntryView#find()}.
       *
       * @param key the key associated with the {@link ReadWriteEntryView} to be
       *            passed to the operation
       * @param argument argument passed in as first parameter to the {@link BiFunction}.
       * @param f operation that takes a user defined value, and a
       *          {@link ReadWriteEntryView} associated with the key, and writes
       *          to the {@link ReadWriteEntryView} passed in, possibly
       *          returning previously stored value or metadata information
       * @param <R> type of the function's return
       * @return a {@link CompletableFuture} which will be completed with the
       *         returned value from the function
       */
      <T, R> CompletableFuture<R> eval(K key, T argument, BiFunction<T, ReadWriteEntryView<K, V>, R> f);

      /**
       * Same as {@link #eval(Object, Object, BiFunction)}  except that the function must also
       * implement <code>Serializable</code>
       * <p>
       * The compiler will pick this overload for lambda parameters, making them <code>Serializable</code>
       */
      default <T, R> CompletableFuture<R> eval(K key, T argument, SerializableBiFunction<T, ReadWriteEntryView<K, V>, R> f) {
         return eval(key, argument, (BiFunction<T, ReadWriteEntryView<K, V>, R>) f);
      }

      /**
       * Evaluate a read-write {@link BiFunction}, with an argument passed in and
       * a {@link ReadWriteEntryView} of the value associated with
       * the key, for each of the keys in the set passed in, and
       * returns an {@link Traversable} to navigate each of the
       * {@link BiFunction} invocation returns.
       *
       * <p>This method can be used to implement operations that store a set of
       * keys and return previous values or metadata parameters.
       *
       * <p>These kind of operations are preferred to traditional end user
       * iterations because the internal logic can often iterate more
       * efficiently since it knows more about the system.
       *
       * <p>Note that when {@link org.infinispan.commons.dataconversion.Encoder encoders}
       * are in place despite the argument type and value type don't have to match
       * the argument will use value encoding.
       *
       * <p>The function must not mutate neither the key returned through
       * {@link ReadEntryView#key()} nor the internally stored value provided
       * through {@link ReadEntryView#get()} or {@link ReadEntryView#find()}.
       *
       * @param arguments the key/value pairs associated with each of the
       *             {@link ReadWriteEntryView} passed in the function callbacks
       * @param f function that takes in a value associated with a key in the
       *          entries collection and the {@link ReadWriteEntryView} associated
       *          with that key in the cache
       * @return a {@link Traversable} to navigate each {@link BiFunction} return
       */
      <T, R> Traversable<R> evalMany(Map<? extends K, ? extends T> arguments, BiFunction<T, ReadWriteEntryView<K, V>, R> f);

      /**
       * Same as {@link #evalMany(Map, BiFunction)}  except that the function must also
       * implement <code>Serializable</code>
       * <p>
       * The compiler will pick this overload for lambda parameters, making them <code>Serializable</code>
       */
      default <T, R> Traversable<R> evalMany(Map<? extends K, ? extends T> arguments, SerializableBiFunction<T, ReadWriteEntryView<K, V>, R> f) {
         return evalMany(arguments, (BiFunction<T, ReadWriteEntryView<K, V>, R>) f);
      }

      /**
       * Evaluate a read-write {@link Function} operation with the
       * {@link ReadWriteEntryView} of the value associated with the key, for each
       * of the keys in the set passed in, and returns a {@link Traversable}
       * to navigate each of the {@link Function} invocation returns.
       *
       * <p>This method can be used to a remove a set of keys returning
       * previous values or metadata parameters.
       *
       * <p>The function must not mutate neither the key returned through
       * {@link ReadEntryView#key()} nor the internally stored value provided
       * through {@link ReadEntryView#get()} or {@link ReadEntryView#find()}.
       *
       * @param keys the keys associated with each of the {@link ReadWriteEntryView}
       *             passed in the function callbacks
       * @param f function that the {@link ReadWriteEntryView} associated with
       *          one of the keys passed in, and returns a value
       * @return a {@link Traversable} to navigate each {@link Function} return
       */
      <R> Traversable<R> evalMany(Set<? extends K> keys, Function<ReadWriteEntryView<K, V>, R> f);

      /**
       * Same as {@link #evalMany(Set, Function)}  except that the function must also
       * implement <code>Serializable</code>
       * <p>
       * The compiler will pick this overload for lambda parameters, making them <code>Serializable</code>
       */
      default <R> Traversable<R> evalMany(Set<? extends K> keys, SerializableFunction<ReadWriteEntryView<K, V>, R> f) {
         return evalMany(keys, (Function<ReadWriteEntryView<K, V>, R>) f);
      }

      /**
       * Evaluate a read-write {@link Function} operation with the
       * {@link ReadWriteEntryView} of the value associated with the key, for all
       * existing keys, and returns a {@link Traversable} to navigate each of
       * the {@link Function} invocation returns.
       *
       * <p>This method can be used to an operation that removes all cached
       * entries individually, and returns previous value and/or metadata
       * parameters.
       *
       * <p>The function must not mutate neither the key returned through
       * {@link ReadEntryView#key()} nor the internally stored value provided
       * through {@link ReadEntryView#get()} or {@link ReadEntryView#find()}.
       *
       * @return a {@link Traversable} to navigate each {@link Function} return
       */
      <R> Traversable<R> evalAll(Function<ReadWriteEntryView<K, V>, R> f);

      /**
       * Same as {@link #evalAll(Function)}   except that the function must also
       * implement <code>Serializable</code>
       * <p>
       * The compiler will pick this overload for lambda parameters, making them <code>Serializable</code>
       */
      default <R> Traversable<R> evalAll(SerializableFunction<ReadWriteEntryView<K, V>, R> f) {
         return evalAll((Function<ReadWriteEntryView<K, V>, R>) f);
      }

      /**
       * Allows to read-write listeners to be registered.
       */
      ReadWriteListeners<K, V> listeners();
   }

}
