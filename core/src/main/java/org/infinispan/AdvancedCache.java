package org.infinispan;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;

import javax.security.auth.Subject;
import javax.transaction.xa.XAResource;

import org.infinispan.batch.BatchContainer;
import org.infinispan.cache.impl.DecoratedCache;
import org.infinispan.commons.api.TransactionalCache;
import org.infinispan.commons.dataconversion.Encoder;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.Wrapper;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.PartitionHandlingConfiguration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.encoding.DataConversion;
import org.infinispan.eviction.EvictionManager;
import org.infinispan.expiration.ExpirationManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.metadata.Metadata;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.partitionhandling.impl.PartitionHandlingManager;
import org.infinispan.persistence.spi.CacheLoader;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.security.AuthorizationManager;
import org.infinispan.stats.Stats;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.function.SerializableBiFunction;
import org.infinispan.util.function.SerializableFunction;

/**
 * An advanced interface that exposes additional methods not available on {@link Cache}.
 *
 * @author Manik Surtani
 * @author Galder Zamarreño
 * @author Tristan Tarrant
 * @since 4.0
 */
public interface AdvancedCache<K, V> extends Cache<K, V>, TransactionalCache {

   /**
    * A method that adds flags to any API call.  For example, consider the following code snippet:
    * <pre>
    *   cache.withFlags(Flag.FORCE_WRITE_LOCK).get(key);
    * </pre>
    * will invoke a cache.get() with a write lock forced.
    * <p/>
    * <b>Note</b> that for the flag to take effect, the cache operation <b>must</b> be invoked on the instance returned
    * by this method.
    * <p/>
    * As an alternative to setting this on every invocation, users should also consider saving the decorated
    * cache, as this allows for more readable code.  E.g.:
    * <pre>
    *    AdvancedCache&lt;?, ?&gt; forceWriteLockCache = cache.withFlags(Flag.FORCE_WRITE_LOCK);
    *    forceWriteLockCache.get(key1);
    *    forceWriteLockCache.get(key2);
    *    forceWriteLockCache.get(key3);
    * </pre>
    *
    * @param flags a set of flags to apply.  See the {@link Flag} documentation.
    * @return an {@link AdvancedCache} instance on which a real operation is to be invoked, if the flags are to be
    * applied.
    */
   AdvancedCache<K, V> withFlags(Flag... flags);

   /**
    * An alternative to {@link #withFlags(Flag...)} not requiring array allocation.
    *
    * @since 9.2
    */
   default AdvancedCache<K, V> withFlags(Collection<Flag> flags) {
      if (flags == null) return this;
      int numFlags = flags.size();
      if (numFlags == 0) return this;
      return withFlags(flags.toArray(new Flag[numFlags]));
   }

   /**
    * An alternative to {@link #withFlags(Flag...)} optimized for a single flag.
    *
    * @since 10.0
    */
   default AdvancedCache<K, V> withFlags(Flag flag) {
      return withFlags(new Flag[]{flag});
   }

   /**
    * Unset all flags set on this cache using {@link #withFlags(Flag...)} or {@link #withFlags(Collection)} methods.
    *
    * @return Cache not applying any flags to the command; possibly <code>this</code>.
    */
   default AdvancedCache<K, V> noFlags() {
      throw new UnsupportedOperationException();
   }

   /**
    * Apply the <code>transformation</code> on each {@link AdvancedCache} instance in a delegation chain, starting
    * with the innermost implementation.
    *
    * @param transformation
    * @return The outermost transformed cache.
    */
   default AdvancedCache<K, V> transform(Function<AdvancedCache<K, V>, ? extends AdvancedCache<K, V>> transformation) {
      throw new UnsupportedOperationException();
   }

   /**
    * Performs any cache operations using the specified {@link Subject}. Only applies to caches with authorization
    * enabled (see {@link ConfigurationBuilder#security()}).
    *
    * @param subject
    * @return an {@link AdvancedCache} instance on which a real operation is to be invoked, using the specified subject
    */
   AdvancedCache<K, V> withSubject(Subject subject);

   /**
    * Allows the modification of the interceptor chain.
    *
    * @deprecated Since 10.0, will be removed without a replacement
    */
   @Deprecated
   AsyncInterceptorChain getAsyncInterceptorChain();

   /**
    * @return the eviction manager - if one is configured - for this cache instance
    *
    * @deprecated Since 10.1, will be removed without a replacement
    */
   @Deprecated
   EvictionManager getEvictionManager();

   /**
    * @return the expiration manager - if one is configured - for this cache instance
    */
   ExpirationManager<K, V> getExpirationManager();

   /**
    * @return the component registry for this cache instance
    * @deprecated Since 10.0, with no public API replacement
    */
   @Deprecated
   ComponentRegistry getComponentRegistry();

   /**
    * Retrieves a reference to the {@link org.infinispan.distribution.DistributionManager} if the cache is configured to
    * use Distribution.  Otherwise, returns a null.
    *
    * @return a DistributionManager, or null.
    */
   DistributionManager getDistributionManager();

   /**
    * Retrieves the {@link AuthorizationManager} if the cache has security enabled. Otherwise returns null
    *
    * @return an AuthorizationManager or null
    */
   AuthorizationManager getAuthorizationManager();

   /**
    * Whenever this cache acquires a lock it will do so using the given Object as the owner of said lock.
    * <p>
    * This can be useful when a lock may have been manually acquired and you wish to reuse that lock across
    * invocations.
    * <p>
    * Great care should be taken with this command as misuse can very easily lead to deadlocks.
    *
    * @param lockOwner the lock owner to lock any keys as
    * @return an {@link AdvancedCache} instance on which when an operation is invoked it will use lock owner object to
    * acquire any locks
    */
   AdvancedCache<K, V> lockAs(Object lockOwner);

   /**
    * Locks a given key or keys eagerly across cache nodes in a cluster.
    * <p>
    * Keys can be locked eagerly in the context of a transaction only.
    *
    * @param keys the keys to lock
    * @return true if the lock acquisition attempt was successful for <i>all</i> keys; false will only be returned if
    * the lock acquisition timed out and the operation has been called with {@link Flag#FAIL_SILENTLY}.
    * @throws org.infinispan.util.concurrent.TimeoutException if the lock cannot be acquired within the configured lock
    *                                                         acquisition time.
    */
   boolean lock(K... keys);

   /**
    * Locks collections of keys eagerly across cache nodes in a cluster.
    * <p>
    * Collections of keys can be locked eagerly in the context of a transaction only.
    *
    * @param keys collection of keys to lock
    * @return true if the lock acquisition attempt was successful for <i>all</i> keys; false will only be returned if
    * the lock acquisition timed out and the operation has been called with {@link Flag#FAIL_SILENTLY}.
    * @throws org.infinispan.util.concurrent.TimeoutException if the lock cannot be acquired within the configured lock
    *                                                         acquisition time.
    */
   boolean lock(Collection<? extends K> keys);


   /**
    * Returns the component in charge of communication with other caches in the cluster.  If the cache's {@link
    * org.infinispan.configuration.cache.ClusteringConfiguration#cacheMode()} is {@link
    * org.infinispan.configuration.cache.CacheMode#LOCAL}, this method will return null.
    *
    * @return the RPC manager component associated with this cache instance or null
    */
   RpcManager getRpcManager();

   /**
    * Returns the component in charge of batching cache operations.
    *
    * @return the batching component associated with this cache instance
    */
   BatchContainer getBatchContainer();

   /**
    * Returns the container where data is stored in the cache. Users should interact with this component with care
    * because direct calls on it bypass the internal interceptors and other infrastructure in place to guarantee the
    * consistency of data.
    *
    * @return the data container associated with this cache instance
    */
   DataContainer<K, V> getDataContainer();

   /**
    * Returns the component that deals with all aspects of acquiring and releasing locks for cache entries.
    *
    * @return retrieves the lock manager associated with this cache instance
    */
   LockManager getLockManager();

   /**
    * Returns a {@link Stats} object that allows several statistics associated with this cache at runtime.
    *
    * @return this cache's {@link Stats} object
    */
   Stats getStats();

   /**
    * Returns the {@link XAResource} associated with this cache which can be used to do transactional recovery.
    *
    * @return an instance of {@link XAResource}
    */
   XAResource getXAResource();

   /**
    * Returns the cache loader associated associated with this cache.  As an alternative to setting this on every
    * invocation, users could also consider using the {@link DecoratedCache} wrapper.
    *
    * @return this cache's cache loader
    */
   ClassLoader getClassLoader();

   /**
    * @deprecated Since 9.4, unmarshalling always uses the classloader from the global configuration.
    */
   @Deprecated
   AdvancedCache<K, V> with(ClassLoader classLoader);

   /**
    * An overloaded form of {@link #put(K, V)}, which takes in an instance of {@link org.infinispan.metadata.Metadata}
    * which can be used to provide metadata information for the entry being stored, such as lifespan, version of
    * value...etc.
    *
    * @param key      key to use
    * @param value    value to store
    * @param metadata information to store alongside the value
    * @return the previous value associated with <tt>key</tt>, or <tt>null</tt> if there was no mapping for
    * <tt>key</tt>.
    * @since 5.3
    */
   V put(K key, V value, Metadata metadata);

   default CompletionStage<CacheEntry<K, V>> putAsyncReturnEntry(K key, V value, Metadata metadata) {
      // TODO: implement later
      return putAsync(key, value, metadata)
            .thenApply(prev -> prev != null ? new ImmortalCacheEntry(key, prev) : null);
   }

   /**
    * An overloaded form of {@link #putAll(Map)}, which takes in an instance of {@link org.infinispan.metadata.Metadata}
    * which can be used to provide metadata information for the entries being stored, such as lifespan, version of
    * value...etc.
    *
    * @param map the values to store
    * @param metadata information to store alongside the value(s)
    * @since 7.2
    */
   void putAll(Map<? extends K, ? extends V> map, Metadata metadata);

   default CompletableFuture<Void> putAllAsync(Map<? extends K, ? extends V> map, Metadata metadata) {
      return putAllAsync(map, metadata.lifespan(), TimeUnit.MILLISECONDS, metadata.maxIdle(), TimeUnit.MILLISECONDS);
   }

   /**
    * An overloaded form of {@link #replace(K, V)}, which takes in an instance of {@link Metadata} which can be used to
    * provide metadata information for the entry being stored, such as lifespan, version of value...etc. The {@link
    * Metadata} is only stored if the call is successful.
    *
    * @param key      key with which the specified value is associated
    * @param value    value to be associated with the specified key
    * @param metadata information to store alongside the new value
    * @return the previous value associated with the specified key, or <tt>null</tt> if there was no mapping for the
    * key.
    * @since 5.3
    */
   V replace(K key, V value, Metadata metadata);

   /**
    * An overloaded form of {@link #replaceAsync(K, V)}, which takes in an instance of {@link Metadata} which can be used to
    * provide metadata information for the entry being stored, such as lifespan, version of value...etc. The {@link
    * Metadata} is only stored if the call is successful.
    *
    * @param key      key with which the specified value is associated
    * @param value    value to be associated with the specified key
    * @param metadata information to store alongside the new value
    * @return the future that contains previous value associated with the specified key, or <tt>null</tt>
    *    if there was no mapping for the key.
    * @since 9.2
    */
   default CompletableFuture<V> replaceAsync(K key, V value, Metadata metadata) {
      return replaceAsync(key, value, metadata.lifespan(), TimeUnit.MILLISECONDS, metadata.maxIdle(), TimeUnit.MILLISECONDS);
   }

   default CompletionStage<CacheEntry<K, V>> replaceAsyncReturnEntry(K key, V value, Metadata metadata) {
      // TODO: implement later
      return replaceAsync(key, value, metadata)
            .thenApply(prev -> new ImmortalCacheEntry(key, prev));
   }

   /**
    * An overloaded form of {@link #replace(K, V, V)}, which takes in an instance of {@link Metadata} which can be used
    * to provide metadata information for the entry being stored, such as lifespan, version of value...etc. The {@link
    * Metadata} is only stored if the call is successful.
    *
    * @param key      key with which the specified value is associated
    * @param oldValue value expected to be associated with the specified key
    * @param newValue value to be associated with the specified key
    * @param metadata information to store alongside the new value
    * @return <tt>true</tt> if the value was replaced
    * @since 5.3
    */
   boolean replace(K key, V oldValue, V newValue, Metadata metadata);

   default CompletableFuture<Boolean> replaceAsync(K key, V oldValue, V newValue, Metadata metadata) {
      return replaceAsync(key, oldValue, newValue, metadata.lifespan(), TimeUnit.MILLISECONDS, metadata.maxIdle(), TimeUnit.MILLISECONDS);
   }

   /**
    * An overloaded form of {@link #putIfAbsent(K, V)}, which takes in an instance of {@link Metadata} which can be used
    * to provide metadata information for the entry being stored, such as lifespan, version of value...etc. The {@link
    * Metadata} is only stored if the call is successful.
    *
    * @param key      key with which the specified value is to be associated
    * @param value    value to be associated with the specified key
    * @param metadata information to store alongside the new value
    * @return the previous value associated with the specified key, or <tt>null</tt> if there was no mapping for the
    * key.
    * @since 5.3
    */
   V putIfAbsent(K key, V value, Metadata metadata);

   /**
    * An overloaded form of {@link #putIfAbsentAsync(K, V)}, which takes in an instance of {@link Metadata} which can be used
    * to provide metadata information for the entry being stored, such as lifespan, version of value...etc. The {@link
    * Metadata} is only stored if the call is successful.
    *
    * @param key      key with which the specified value is to be associated
    * @param value    value to be associated with the specified key
    * @param metadata information to store alongside the new value
    * @return A future containing the previous value associated with the specified key, or <tt>null</tt> if there was no mapping for the
    * key.
    * @since 9.2
    */
   default CompletableFuture<V> putIfAbsentAsync(K key, V value, Metadata metadata) {
      return putIfAbsentAsync(key, value, metadata.lifespan(), TimeUnit.MILLISECONDS, metadata.maxIdle(), TimeUnit.MILLISECONDS);
   }

   default CompletionStage<CacheEntry<K, V>> putIfAbsentAsyncReturnEntry(K key, V value, Metadata metadata) {
      // TODO: replace with a concurrent operation to do this
      return getCacheEntryAsync(key)
            .thenCompose(ce -> {
               if (ce != null) {
                  return CompletableFuture.completedFuture(ce);
               }
               return putIfAbsentAsync(key, value, metadata)
                     .thenApply(prev -> {
                        if (prev == null) {
                           return null;
                        }
                        return new ImmortalCacheEntry(key, prev);
                     });
            });
   }

   /**
    * An overloaded form of {@link #putForExternalRead(K, V)}, which takes in an instance of {@link Metadata} which can
    * be used to provide metadata information for the entry being stored, such as lifespan, version of value...etc. The
    * {@link Metadata} is only stored if the call is successful.
    *
    * @param key      key with which the specified value is to be associated
    * @param value    value to be associated with the specified key
    * @param metadata information to store alongside the new value
    * @since 7.0
    */
   void putForExternalRead(K key, V value, Metadata metadata);

   /**
    * An overloaded form of {@link #compute(K, BiFunction)}, which takes in an instance of {@link Metadata} which can be
    * used to provide metadata information for the entry being stored, such as lifespan, version of value...etc.
    *
    * @param key               key with which the specified value is associated
    * @param remappingFunction function to be applied to the specified key/value
    * @param metadata          information to store alongside the new value
    * @return the previous value associated with the specified key, or <tt>null</tt> if remapping function is gives
    * null.
    * @since 9.1
    */
   V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, Metadata metadata);

   /**
    * Overloaded {@link #compute(Object, BiFunction, Metadata)} with {@link SerializableBiFunction}
    */
   default V compute(K key, SerializableBiFunction<? super K, ? super V, ? extends V> remappingFunction, Metadata metadata) {
      return this.compute(key, (BiFunction<? super K, ? super V, ? extends V>) remappingFunction, metadata);
   }

   /**
    * An overloaded form of {@link #computeIfPresent(K, BiFunction)}, which takes in an instance of {@link Metadata}
    * which can be used to provide metadata information for the entry being stored, such as lifespan, version of
    * value...etc. The {@link Metadata} is only stored if the call is successful.
    *
    * @param key               key with which the specified value is associated
    * @param remappingFunction function to be applied to the specified key/value
    * @param metadata          information to store alongside the new value
    * @return the previous value associated with the specified key, or <tt>null</tt> if there was no mapping for the
    * key.
    * @since 9.1
    */
   V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, Metadata metadata);

   /**
    * Overloaded {@link #computeIfPresent(Object, BiFunction, Metadata)} with {@link SerializableBiFunction}
    */
   default V computeIfPresent(K key, SerializableBiFunction<? super K, ? super V, ? extends V> remappingFunction, Metadata metadata) {
      return this.computeIfPresent(key, (BiFunction<? super K, ? super V, ? extends V>) remappingFunction, metadata);
   }

   /**
    * An overloaded form of {@link #computeIfAbsent(K, Function)}, which takes in an instance of {@link Metadata} which
    * can be used to provide metadata information for the entry being stored, such as lifespan, version of value...etc.
    * The {@link Metadata} is only stored if the call is successful.
    *
    * @param key             key with which the specified value is associated
    * @param mappingFunction function to be applied to the specified key
    * @param metadata        information to store alongside the new value
    * @return the value created with the mapping function associated with the specified key, or the previous value
    * associated with the specified key if the key is not absent.
    * @since 9.1
    */
   V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction, Metadata metadata);

   /**
    * Overloaded {@link #computeIfAbsent(Object, Function, Metadata)} with {@link SerializableFunction}
    */
   default V computeIfAbsent(K key, SerializableFunction<? super K, ? extends V> mappingFunction, Metadata metadata) {
      return this.computeIfAbsent(key, (Function<? super K, ? extends V>) mappingFunction, metadata);
   }

   /**
    * An overloaded form of {@link #merge(Object, Object, BiFunction)}, which takes in an instance of {@link Metadata}
    * which can be used to provide metadata information for the entry being stored, such as lifespan, version of
    * value...etc. The {@link Metadata} is only stored if the call is successful.
    *
    * @param key,               key with which the resulting value is to be associated
    * @param value,             the non-null value to be merged with the existing value associated with the key or, if
    *                           no existing value or a null value is associated with the key, to be associated with the
    *                           key
    * @param remappingFunction, the function to recompute a value if present
    * @param metadata,          information to store alongside the new value
    * @return the new value associated with the specified key, or null if no value is associated with the key
    * @since 9.2
    */
   V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction, Metadata metadata);

   /**
    * Overloaded {@link #merge(Object, Object, BiFunction, Metadata)} with {@link SerializableBiFunction}
    */
   default V merge(K key, V value, SerializableBiFunction<? super V, ? super V, ? extends V> remappingFunction, Metadata metadata) {
      return this.merge(key, value, (BiFunction<? super V, ? super V, ? extends V>) remappingFunction, metadata);
   }

   /**
    * Asynchronous version of {@link #put(Object, Object, Metadata)} which stores metadata alongside the value.  This
    * method does not block on remote calls, even if your cache mode is synchronous.  Has no benefit over {@link
    * #put(Object, Object, Metadata)} if used in LOCAL mode.
    * <p/>
    *
    * @param key      key to use
    * @param value    value to store
    * @param metadata information to store alongside the new value
    * @return a future containing the old value replaced.
    * @since 5.3
    */
   CompletableFuture<V> putAsync(K key, V value, Metadata metadata);

   /**
    * Overloaded {@link #computeAsync(K, BiFunction)}, which stores metadata alongside the value.  This
    * method does not block on remote calls, even if your cache mode is synchronous.
    *
    * @param key               key with which the specified value is associated
    * @param remappingFunction function to be applied to the specified key/value
    * @param metadata          information to store alongside the new value
    * @return the previous value associated with the specified key, or <tt>null</tt> if remapping function is gives
    * null.
    * @since 9.4
    */
   CompletableFuture<V> computeAsync(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, Metadata metadata);

   /**
    * Overloaded {@link #computeAsync(Object, BiFunction, Metadata)} with {@link SerializableBiFunction}
    * @since 9.4
    */
   default CompletableFuture<V> computeAsync(K key, SerializableBiFunction<? super K, ? super V, ? extends V> remappingFunction, Metadata metadata) {
      return this.computeAsync(key, (BiFunction<? super K, ? super V, ? extends V>) remappingFunction, metadata);
   }

   /**
    * Overloaded {@link #computeIfPresentAsync(K, BiFunction)}, which takes in an instance of {@link Metadata}
    * which can be used to provide metadata information for the entry being stored, such as lifespan, version of
    * value...etc. The {@link Metadata} is only stored if the call is successful.
    *
    * @param key               key with which the specified value is associated
    * @param remappingFunction function to be applied to the specified key/value
    * @param metadata          information to store alongside the new value
    * @return the previous value associated with the specified key, or <tt>null</tt> if there was no mapping for the
    * key.
    * @since 9.4
    */
   CompletableFuture<V> computeIfPresentAsync(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, Metadata metadata);

   /**
    * Overloaded {@link #computeIfPresentAsync(Object, BiFunction, Metadata)} with {@link SerializableBiFunction}
    * @since 9.4
    */
   default CompletableFuture<V> computeIfPresentAsync(K key, SerializableBiFunction<? super K, ? super V, ? extends V> remappingFunction, Metadata metadata) {
      return this.computeIfPresentAsync(key, (BiFunction<? super K, ? super V, ? extends V>) remappingFunction, metadata);
   }

   /**
    * Overloaded {@link #computeIfAbsentAsync(K, Function)}, which takes in an instance of {@link Metadata} which
    * can be used to provide metadata information for the entry being stored, such as lifespan, version of value...etc.
    * The {@link Metadata} is only stored if the call is successful.
    *
    * @param key             key with which the specified value is associated
    * @param mappingFunction function to be applied to the specified key
    * @param metadata        information to store alongside the new value
    * @return the value created with the mapping function associated with the specified key, or the previous value
    * associated with the specified key if the key is not absent.
    * @since 9.4
    */
   CompletableFuture<V> computeIfAbsentAsync(K key, Function<? super K, ? extends V> mappingFunction, Metadata metadata);

   /**
    * Overloaded {@link #computeIfAbsentAsync(Object, Function, Metadata)} with {@link SerializableFunction}
    * @since 9.4
    */
   default CompletableFuture<V> computeIfAbsentAsync(K key, SerializableFunction<? super K, ? extends V> mappingFunction, Metadata metadata) {
      return this.computeIfAbsentAsync(key, (Function<? super K, ? extends V>) mappingFunction, metadata);
   }

   /**
    * Overloaded {@link #mergeAsync(Object, Object, BiFunction)}, which takes in an instance of {@link Metadata}
    * which can be used to provide metadata information for the entry being stored, such as lifespan, version of
    * value...etc. The {@link Metadata} is only stored if the call is successful.
    *
    * @param key,               key with which the resulting value is to be associated
    * @param value,             the non-null value to be merged with the existing value associated with the key or, if
    *                           no existing value or a null value is associated with the key, to be associated with the
    *                           key
    * @param remappingFunction, the function to recompute a value if present
    * @param metadata,          information to store alongside the new value
    * @return the new value associated with the specified key, or null if no value is associated with the key
    * @since 9.4
    */
   CompletableFuture<V> mergeAsync(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction, Metadata metadata);

   /**
    * Overloaded {@link #mergeAsync(Object, Object, BiFunction, Metadata)} with {@link SerializableBiFunction}
    */
   default CompletableFuture<V> mergeAsync(K key, V value, SerializableBiFunction<? super V, ? super V, ? extends V> remappingFunction, Metadata metadata) {
      return this.mergeAsync(key, value, (BiFunction<? super V, ? super V, ? extends V>) remappingFunction, metadata);
   }

   // TODO: Even better: add replace/remove calls that apply the changes if a given function is successful
   // That way, you could do comparison not only on the cache value, but also based on version...etc

   /**
    * Gets a collection of entries, returning them as {@link Map} of the values associated with the set of keys
    * requested.
    * <p>
    * If the cache is configured read-through, and a get for a key would return null because an entry is missing from
    * the cache, the Cache's {@link CacheLoader} is called in an attempt to load the entry. If an entry cannot be loaded
    * for a given key, the returned Map will contain null for value of the key.
    * <p>
    * Unlike other bulk methods if this invoked in an existing transaction all entries will be stored in the current
    * transactional context
    * <p>
    * The returned {@link Map} will be a copy and updates to the map will not be reflected in the Cache and vice versa.
    * The keys and values themselves however may not be copies depending on if storeAsBinary is enabled and the value
    * was retrieved from the local node.
    *
    * @param keys The keys whose associated values are to be returned.
    * @return A map of entries that were found for the given keys. If an entry is not found for a given key, it will not
    * be in the returned map.
    * @throws NullPointerException if keys is null or if keys contains a null
    */
   Map<K, V> getAll(Set<?> keys);

   /**
    * Retrieves a CacheEntry corresponding to a specific key.
    *
    * @param key the key whose associated cache entry is to be returned
    * @return the cache entry to which the specified key is mapped, or {@code null} if this map contains no mapping for
    * the key
    * @since 5.3
    */
   CacheEntry<K, V> getCacheEntry(Object key);

   /**
    * Retrieves a CacheEntry corresponding to a specific key.
    *
    * @param key the key whose associated cache entry is to be returned
    * @return a future with the cache entry to which the specified key is mapped, or with {@code null}
    * if this map contains no mapping for the key
    * @since 9.2
    */
   default CompletableFuture<CacheEntry<K, V>> getCacheEntryAsync(Object key) {
      throw new UnsupportedOperationException("getCacheEntryAsync");
   }

   /**
    * Gets a collection of entries from the {@link AdvancedCache}, returning them as {@link Map} of the cache entries
    * associated with the set of keys requested.
    * <p>
    * If the cache is configured read-through, and a get for a key would return null because an entry is missing from
    * the cache, the Cache's {@link CacheLoader} is called in an attempt to load the entry. If an entry cannot be loaded
    * for a given key, the returned Map will contain null for value of the key.
    * <p>
    * Unlike other bulk methods if this invoked in an existing transaction all entries will be stored in the current
    * transactional context
    * <p>
    * The returned {@link Map} will be a copy and updates to the map will not be reflected in the Cache and vice versa.
    * The keys and values themselves however may not be copies depending on if storeAsBinary is enabled and the value
    * was retrieved from the local node.
    *
    * @param keys The keys whose associated values are to be returned.
    * @return A map of entries that were found for the given keys. Keys not found in the cache are present in the map
    * with null values.
    * @throws NullPointerException if keys is null or if keys contains a null
    */
   Map<K, CacheEntry<K, V>> getAllCacheEntries(Set<?> keys);

   /**
    * Executes an equivalent of {@link Map#putAll(Map)}, returning previous values of the modified entries.
    *
    * @param map mappings to be stored in this map
    * @return A map of previous values for the given keys. If the previous mapping does not exist it will not be in the
    * returned map.
    * @since 9.1
    */
   default Map<K, V> getAndPutAll(Map<? extends K, ? extends V> map) {
      Map<K, V> result = new HashMap<>(map.size());
      for (Entry<? extends K, ? extends V> entry : map.entrySet()) {
         V prev = put(entry.getKey(), entry.getValue());
         if (prev != null) {
            result.put(entry.getKey(), prev);
         }
      }
      return result;
   }

   /**
    * It fetches all the keys which belong to the group.
    * <p/>
    * Semantically, it iterates over all the keys in memory and persistence, and performs a read operation in the keys
    * found. Multiple invocations inside a transaction ensures that all the keys previous read are returned and it may
    * return newly added keys to the group from other committed transactions (also known as phantom reads).
    * <p/>
    * The {@code map} returned is immutable and represents the group at the time of the invocation. If you want to add
    * or remove keys from a group use {@link #put(Object, Object)} and {@link #remove(Object)}. To remove all the keys
    * in the group use {@link #removeGroup(String)}.
    * <p/>
    * To improve performance you may use the {@code flag} {@link org.infinispan.context.Flag#SKIP_CACHE_LOAD} to avoid
    * fetching the key/value from persistence. However, you will get an inconsistent snapshot of the group.
    *
    * @param groupName the group name.
    * @return an immutable {@link java.util.Map} with the key/value pairs.
    */
   Map<K, V> getGroup(String groupName);

   /**
    * It removes all the key which belongs to a group.
    * <p/>
    * Semantically, it fetches the most recent group keys/values and removes them.
    * <p/>
    * Note that, concurrent addition perform by other transactions/threads to the group may not be removed.
    *
    * @param groupName the group name.
    */
   void removeGroup(String groupName);

   /**
    * Returns the cache's availability. In local mode this method will always return {@link AvailabilityMode#AVAILABLE}.
    * In clustered mode, the {@link PartitionHandlingManager} is queried to obtain the availability mode.
    */
   AvailabilityMode getAvailability();

   /**
    * Manually change the availability of the cache. Doesn't change anything if the cache is not clustered or {@link
    * PartitionHandlingConfiguration#whenSplit()} is set to {@link org.infinispan.partitionhandling.PartitionHandling#ALLOW_READ_WRITES}.
    */
   void setAvailability(AvailabilityMode availabilityMode);

   /**
    * Touches the given key if present. This will refresh its last access time, used for max idle, and count as a recent
    * access for eviction purposes.
    * <p>
    * Note that it is possible to touch an entry that is expired via max idle if {@code touchEvenIfExpired} argument is
    * {@code true}.
    * <p>
    * This method will return without blocking and complete the returned stage with a value after all appropriate nodes
    * have actually touched the value.
    * @param key key of the entry to touch
    * @param touchEvenIfExpired true if the entry should be touched even if already expired via max idle, effectively
    *                           making it so the entry is no longer expired via max idle
    * @return true if the entry was actually touched
    */
   CompletionStage<Boolean> touch(Object key, boolean touchEvenIfExpired);

   /**
    * The same as {@link #touch(Object, boolean)} except that the segment is already known. This can be helpful to reduce
    * an extra segment computation
    * @param key key of the entry to touch
    * @param segment segment of the key
    * @param touchEvenIfExpired true if the entry should be touched even if already expired via max idle, effectively
    *                           making it so the entry is no longer expired via max idle
    * @return true if the entry was actually touched
    */
   CompletionStage<Boolean> touch(Object key, int segment, boolean touchEvenIfExpired);

   /**
    * Identical to {@link Cache#entrySet()} but is typed to return CacheEntries instead of Entries.  Please see the
    * other method for a description of its behaviors.
    * <p>
    * This method is needed since nested generics do not support covariance
    *
    * @return the entry set containing all of the CacheEntries
    * @see Cache#entrySet()
    */
   CacheSet<CacheEntry<K, V>> cacheEntrySet();

   /**
    * Returns a sequential stream using this Cache as the source. This stream is very similar to using the {@link
    * CacheStream} returned from the {@link CacheSet#stream()} method of the collection returned via {@link
    * AdvancedCache#cacheEntrySet()}. The use of this locked stream is that when an entry is being processed by the user
    * the entry is locked for the invocation preventing a different thread from modifying it.
    * <p>
    * Note that this stream is not supported when using a optimistic transactional or simple cache. Both non
    * transactional and pessimistic transactional caches are supported.
    * <p>
    * The stream will not share any ongoing transaction the user may have. Code executed by the stream should be treated
    * as completely independent. That is any operation performed via the stream will require the user to start their own
    * transaction or will be done intrinsically on the invocation. Note that if there is an ongoing transaction that has
    * a lock on a key from the cache, that it will cause a deadlock.
    * <p>
    * Currently simple cache, {@link org.infinispan.configuration.cache.ConfigurationBuilder#simpleCache(boolean)} was
    * set to true, and optimistic caches, {@link org.infinispan.configuration.cache.TransactionConfigurationBuilder#lockingMode(LockingMode)}
    * was set to {@link LockingMode#OPTIMISTIC}, do not support this method. In this case it will throw an {@link
    * UnsupportedOperationException}. This restriction may be removed in a future version. Also this method cannot be
    * used on a cache that has a lock owner already specified via {@link AdvancedCache#lockAs(Object)} as this could
    * lead to a deadlock or the release of locks early and will throw an {@link IllegalStateException}.
    *
    * @return the locked stream
    * @throws UnsupportedOperationException this is thrown if invoked from a cache that doesn't support this
    * @throws IllegalStateException         if this cache has already explicitly set a lock owner
    * @since 9.1
    */
   LockedStream<K, V> lockedStream();

   /**
    * Attempts to remove the entry if it is expired.  Due to expired entries not being consistent across nodes, this
    * will still attempt to remove the value if it is not present.  Note that this will raise an expired event even if
    * the entry is not present.  Normally this method should never be invoked except by the {@link ExpirationManager}.
    * <p>
    * This command will only remove the value if the value and lifespan also match if provided.
    * <p>
    * This method will suspend any ongoing transaction and start a new one just for the invocation of this command. It
    * is automatically committed or rolled back after the command completes, either successfully or via an exception.
    * <p>
    * NOTE: This method may be removed at any point including in a minor release and is not supported for external
    * usage.
    *
    * @param key      the key that is expiring
    * @param value    the value that mapped to the given.  Null means it will match any value
    * @param lifespan the lifespan that should match.  If null is provided it will match any lifespan value
    * @return if the entry was removed
    */
   CompletableFuture<Boolean> removeLifespanExpired(K key, V value, Long lifespan);

   /**
    * Attempts to remove the entry for the given key, if it has expired due to max idle. This command first locks
    * the key and then verifies that the entry has expired via maxIdle across all nodes. If it has this will then
    * remove the given key.
    * <p>
    * This method returns a boolean when it has determined if the entry has expired. This is useful for when a backup
    * node invokes this command for a get that found the entry expired. This way the node can return back to the caller
    * much faster when the entry is not expired and do any additional processing asynchronously if needed.
    * <p>
    * This method will suspend any ongoing transaction and start a new one just for the invocation of this command. It
    * is automatically committed or rolled back after the command completes, either successfully or via an exception.
    * <p>
    * NOTE: This method may be removed at any point including in a minor release and is not supported for external
    * usage.
    * @param key the key that expired via max idle for the given entry
    * @return if the entry was removed
    */
   CompletableFuture<Boolean> removeMaxIdleExpired(K key, V value);

   default CompletionStage<CacheEntry<K, V>> removeAsyncReturnEntry(K key) {
      // TODO: replace this with an actual impl
      return removeAsync(key)
            .thenApply(prev -> prev == null ? null : new ImmortalCacheEntry(key, prev));
   }

   /**
    * Performs any cache operations using the specified pair of {@link Encoder}.
    *
    * @param keyEncoder {@link Encoder} for the keys.
    * @param valueEncoder {@link Encoder} for the values.
    * @return an instance of {@link AdvancedCache} where all operations will use the supplied encoders.
    * @deprecated Since 12.1, to be removed in a future version.
    */
   @Deprecated
   AdvancedCache<?, ?> withEncoding(Class<? extends Encoder> keyEncoder, Class<? extends Encoder> valueEncoder);

   /**
    * Performs any cache operations using the specified pair of {@link Wrapper}.
    *
    * @param keyWrapper   {@link Wrapper} for the keys.
    * @param valueWrapper {@link Wrapper} for the values.
    * @return {@link AdvancedCache} where all operations will use the supplied wrappers.
    * @deprecated Since 11.0. To be removed in 14.0, with no replacement.
    */
   AdvancedCache<K, V> withWrapping(Class<? extends Wrapper> keyWrapper, Class<? extends Wrapper> valueWrapper);

   /**
    * Performs any cache operations using the specified {@link Encoder}.
    *
    * @param encoder {@link Encoder} used for both keys and values.
    * @return an instance of {@link AdvancedCache} where all operations will use the supplied encoder.
    * @deprecated Since 12.1, to be removed in a future version.
    */
   @Deprecated
   AdvancedCache<?, ?> withEncoding(Class<? extends Encoder> encoder);

   /**
    * Performs any cache operations using the specified {@link Wrapper}.
    *
    * @param wrapper {@link Wrapper} for the keys and values.
    * @return an instance of {@link AdvancedCache} where all operations will use the supplied wrapper.
    * @deprecated Since 11.0. To be removed in 14.0, with no replacement.
    */
   AdvancedCache<K, V> withWrapping(Class<? extends Wrapper> wrapper);

   /**
    * Perform any cache operations using an alternate {@link org.infinispan.commons.dataconversion.MediaType}.
    *
    * @param keyMediaType   {@link org.infinispan.commons.dataconversion.MediaType} for the keys.
    * @param valueMediaType {@link org.infinispan.commons.dataconversion} for the values.
    * @return an instance of {@link AdvancedCache} where all data will formatted according to the supplied {@link
    * org.infinispan.commons.dataconversion.MediaType}.
    *
    * @deprecated Use {@link #withMediaType(MediaType, MediaType)} instead.
    */
   @Deprecated
   AdvancedCache<?, ?> withMediaType(String keyMediaType, String valueMediaType);

   /**
    * @see #withMediaType(String, String)
    */
   <K1, V1> AdvancedCache<K1, V1> withMediaType(MediaType keyMediaType, MediaType valueMediaType);

   /**
    * Perform any cache operations using the same {@link org.infinispan.commons.dataconversion.MediaType} of the cache
    * storage. This is equivalent to disabling transcoding on the cache.
    *
    * @return an instance of {@link AdvancedCache} where no data conversion will take place.
    */
   AdvancedCache<K, V> withStorageMediaType();

   /**
    * @return The associated {@link DataConversion} for the keys.
    */
   DataConversion getKeyDataConversion();

   /**
    * @return The associated {@link DataConversion} for the cache's values.
    */
   DataConversion getValueDataConversion();

   /**
    * @deprecated Since 12.1, to be removed in a future version.
    */
   @Deprecated
   AdvancedCache<?, ?> withKeyEncoding(Class<? extends Encoder> encoder);
}
