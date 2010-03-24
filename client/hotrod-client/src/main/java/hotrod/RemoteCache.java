package hotrod;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.util.concurrent.NotifyingFuture;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Provides remote reference to a Hot Rod server/cluster. It implements {@link org.infinispan.Cache}, but given its
 * nature (remote) some operations are not supported. All these unsupported operations are being overridden within this
 * interface and documented as such.
 * <p/>
 * <b>New operations</b>: besides the operations inherited from {@link org.infinispan.Cache}, RemoteCache also adds new
 * operations to optimize/reduce network traffic: e.g. versioned put operation.
 * <p/>
 * <b>Concurrency</b>: implementors of this interface will support multi-threaded access, similar to the way {@link
 * org.infinispan.Cache} supports it.
 * <p/>
 * <b>Return values</b>: previous existing values for certain {@link java.util.Map} operations are not being returned,
 * but null will always be returned instead. E.g. {@link java.util.Map#put(Object, Object)} returns the previous value
 * associated to the supplied key. In case of RemoteCache, this will always return null.
 * <p/>
 * <b>Synthetic operations</b>: Certain aggregate operations are being implemented based on other Hot Rod operations.
 * E.g. all the {@link java.util.Map#putAll(java.util.Map)} is implemented through multiple individual puts. This means
 * that the these operations are not atomic and that they are costly, e.g. as the number of network round-trips is not
 * one, but the size of the added map. All these synthetic operations are documented as such and should be used
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public interface RemoteCache<K, V> extends Cache<K, V> {


   /**
    * Removes the given entry only if its version matches the supplied version. A typical use case looks like this:
    * <pre>
    * VersionedEntry ve = remoteCache.getVersionedEntry(key);
    * //some processing
    * remoteCache.remove(key, ve.getVersion();
    * </pre>
    * Lat call (remove) will make sure that the entry will only be removed if it hasn't been changed in between.
    *
    * @return true if the entry has been removed
    * @see VersionedEntry
    * @see #getVersionedEntry(Object)
    */
   boolean remove(Object key, long version);

   /**
    * @see #remove(Object, Object)
    */
   NotifyingFuture<Boolean> removeAsync(Object key, long version);

   /**
    * Removes the given value only if its version matches the supplied version. See {@link #remove(Object, long)} for a
    * sample usage.
    *
    * @return true if the method has been replaced
    * @see #getVersionedEntry(Object)
    * @see VersionedEntry
    */
   boolean replace(K key, V newValue, long version);

   /**
    * @see #replace(Object, Object, long)
    */
   boolean replace(K key, V newValue, long version, long lifespan, TimeUnit unit);

   /**
    * @see #replace(Object, Object, long)
    */
   boolean replace(K key, V newValue, long version, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit);

   /**
    * @see #replace(Object, Object, long)
    */
   NotifyingFuture<Boolean> replaceAsync(K key, V newValue, long version);

   /**
    * @see #replace(Object, Object, long)
    */
   NotifyingFuture<Boolean> replaceAsync(K key, V newValue, long version, long lifespan, TimeUnit unit);

   /**
    * @see #replace(Object, Object, long)
    */
   NotifyingFuture<Boolean> replaceAsync(K key, V newValue, long version, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit);


   /**
    * Returns the {@link VersionedEntry} associated to the supplied key param, or null if it doesn't exist.
    */
   VersionedEntry getVersionedEntry(K key);


   /**
    * Operation might be supported for smart clients that will be able to register for topology changes.
    *
    * @throws UnsupportedOperationException
    */
   @Override
   void addListener(Object listener);

   /**
    * @throws UnsupportedOperationException
    * @see #addListener(Object)
    */
   @Override
   void removeListener(Object listener);

   /**
    * @throws UnsupportedOperationException
    * @see #addListener(Object)
    */
   @Override
   Set<Object> getListeners();

   /**
    * @throws UnsupportedOperationException
    */
   @Override
   int size();

   /**
    * @throws UnsupportedOperationException
    */
   @Override
   boolean isEmpty();

   /**
    * @throws UnsupportedOperationException
    */
   @Override
   boolean containsValue(Object value);

   /**
    * @throws UnsupportedOperationException
    */
   @Override
   Set<K> keySet();

   /**
    * @throws UnsupportedOperationException
    */
   @Override
   Collection<V> values();

   /**
    * @throws UnsupportedOperationException
    */
   @Override
   Set<Entry<K, V>> entrySet();

   /**
    * @throws UnsupportedOperationException
    */
   @Override
   void evict(K key);

   /**
    * @throws UnsupportedOperationException
    */
   @Override
   Configuration getConfiguration();

   /**
    * @throws UnsupportedOperationException
    */
   @Override
   boolean startBatch();

   /**
    * @throws UnsupportedOperationException
    */
   @Override
   void endBatch(boolean successful);

   /**
    * This operation is not supported. Consider using {@link #remove(Object, long)} instead.
    *
    * @throws UnsupportedOperationException
    */
   @Override
   boolean remove(Object key, Object value);

   /**
    * This operation is not supported. Consider using {@link #removeAsync(Object, long)} instead.
    *
    * @throws UnsupportedOperationException
    */
   @Override
   NotifyingFuture<Boolean> removeAsync(Object key, Object value);

   /**
    * This operation is not supported. Consider using {@link #replace(Object, Object, long)} instead.
    *
    * @throws UnsupportedOperationException
    */
   @Override
   boolean replace(K key, V oldValue, V newValue);

   /**
    * This operation is not supported. Consider using {@link #replace(Object, Object, long, long, TimeUnit)} instead.
    *
    * @throws UnsupportedOperationException
    */
   @Override
   boolean replace(K key, V oldValue, V value, long lifespan, TimeUnit unit);

   /**
    * This operation is not supported. Consider using {@link #replace(Object, Object, long, long, TimeUnit, long,
    * TimeUnit)} instead.
    *
    * @throws UnsupportedOperationException
    */
   @Override
   boolean replace(K key, V oldValue, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit);

   /**
    * This operation is not supported. Consider using {@link #replaceAsync(Object, Object, long)} instead.
    *
    * @throws UnsupportedOperationException
    */
   @Override
   NotifyingFuture<Boolean> replaceAsync(K key, V oldValue, V newValue);

   /**
    * This operation is not supported. Consider using {@link #replaceAsync(Object, Object, long, long, TimeUnit)}
    * instead.
    *
    * @throws UnsupportedOperationException
    */
   @Override
   NotifyingFuture<Boolean> replaceAsync(K key, V oldValue, V newValue, long lifespan, TimeUnit unit);

   /**
    * This operation is not supported. Consider using {@link #replaceAsync(Object, Object, long, long, TimeUnit, long,
    * TimeUnit)} instead.
    *
    * @throws UnsupportedOperationException
    */
   @Override
   NotifyingFuture<Boolean> replaceAsync(K key, V oldValue, V newValue, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit);

   /**
    * This operation is not supported.
    *
    * @throws UnsupportedOperationException
    */
   @Override
   AdvancedCache<K, V> getAdvancedCache();

   /**
    * This operation is not supported.
    *
    * @throws UnsupportedOperationException
    */
   @Override
   void compact();


   /**
    * Synthetic operation. The client iterates over the set of keys and calls put for each one of them. This results in
    * operation not being atomic (if a failure happens after few puts it is not rolled back) and costly (for each key in
    * the parameter map a remote call is performed).
    */
   @Override
   void putAll(Map<? extends K, ? extends V> map, long lifespan, TimeUnit unit);

   /**
    * Synthetic operation.
    *
    * @see #putAll(java.util.Map, long, java.util.concurrent.TimeUnit)
    */
   @Override
   void putAll(Map<? extends K, ? extends V> map, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit);

   /**
    * Synthetic operation.
    *
    * @see #putAll(java.util.Map, long, java.util.concurrent.TimeUnit)
    */
   @Override
   NotifyingFuture<Void> putAllAsync(Map<? extends K, ? extends V> data);

   /**
    * Synthetic operation.
    *
    * @see #putAll(java.util.Map, long, java.util.concurrent.TimeUnit)
    */
   @Override
   NotifyingFuture<Void> putAllAsync(Map<? extends K, ? extends V> data, long lifespan, TimeUnit unit);

   /**
    * Synthetic operation.
    *
    * @see #putAll(java.util.Map, long, java.util.concurrent.TimeUnit)
    */
   @Override
   NotifyingFuture<Void> putAllAsync(Map<? extends K, ? extends V> data, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit);

   /**
    * Synthetic operation.
    *
    * @see #putAll(java.util.Map, long, java.util.concurrent.TimeUnit)
    */
   @Override
   void putAll(Map<? extends K, ? extends V> m);
}
