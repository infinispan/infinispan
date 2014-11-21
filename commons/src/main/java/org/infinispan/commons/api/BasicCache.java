package org.infinispan.commons.api;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * BasicCache provides the common building block for the two different types of caches that Infinispan provides:
 * embedded and remote.
 * <p/>
 * For convenience, BasicCache extends {@link ConcurrentMap} and implements all methods accordingly, although methods like
 * {@link ConcurrentMap#keySet()}, {@link ConcurrentMap#values()} and {@link ConcurrentMap#entrySet()} are expensive
 * (prohibitively so when using a distributed cache) and frequent use of these methods is not recommended.
 * <p />
 * Other methods such as {@link #size()} provide an approximation-only, and should not be relied on for an accurate picture
 * as to the size of the entire, distributed cache.  Remote nodes are <i>not</i> queried and in-fly transactions are not
 * taken into account, even if {@link #size()} is invoked from within such a transaction.
 * <p/>
 * Also, like many {@link ConcurrentMap} implementations, BasicCache does not support the use of <tt>null</tt> keys or
 * values.
 * <p/>
 * <h3>Unsupported operations</h3>
 * <p>{@link #containsValue(Object)}</p>
 *
 * Please see the <a href="http://www.jboss.org/infinispan/docs">Infinispan documentation</a> and/or the <a
 * href="https://docs.jboss.org/author/display/ISPN/Getting+Started+Guide#GettingStartedGuide-5minutetutorial">5 Minute Usage Tutorial</a> for more details.
 * <p/>
 *
 * @author Mircea.Markus@jboss.com
 * @author Manik Surtani
 * @author Galder Zamarreño
 * @author Tristan Tarrant
 *
 * @see org.infinispan.manager.CacheContainer
 * @see DefaultCacheManager
 * @see <a href="http://www.jboss.org/infinispan/docs">Infinispan documentation</a>
 * @see <a href="http://www.jboss.org/community/wiki/5minutetutorialonInfinispan">5 Minute Usage Tutorial</a>
 *
 * @since 5.1
 */
public interface BasicCache<K, V> extends AsyncCache<K, V>, ConcurrentMap<K, V>, Lifecycle {
   /**
    * Retrieves the name of the cache
    *
    * @return the name of the cache
    */
   String getName();

   /**
    * Retrieves the version of Infinispan
    *
    * @return a version string
    */
   String getVersion();

   /**
    * {@inheritDoc}
    *
    * If the return value of this operation will be ignored by the application,
    * the user is strongly encouraged to use the {@link org.infinispan.context.Flag#IGNORE_RETURN_VALUES}
    * flag when invoking this method in order to make it behave as efficiently
    * as possible (i.e. avoiding needless remote or network calls).
    */
   @Override
   V put(K key, V value);

   /**
    * An overloaded form of {@link #put(Object, Object)}, which takes in lifespan parameters.
    *
    * @param key      key to use
    * @param value    value to store
    * @param lifespan lifespan of the entry.  Negative values are interpreted as unlimited lifespan.
    * @param unit     unit of measurement for the lifespan
    * @return the value being replaced, or null if nothing is being replaced.
    */
   V put(K key, V value, long lifespan, TimeUnit unit);

   /**
    * An overloaded form of {@link #putIfAbsent(Object, Object)}, which takes in lifespan parameters.
    *
    * @param key      key to use
    * @param value    value to store
    * @param lifespan lifespan of the entry.  Negative values are interpreted as unlimited lifespan.
    * @param unit     unit of measurement for the lifespan
    * @return the value being replaced, or null if nothing is being replaced.
    */
   V putIfAbsent(K key, V value, long lifespan, TimeUnit unit);

   /**
    * An overloaded form of {@link #putAll(Map)}, which takes in lifespan parameters.  Note that the lifespan is applied
    * to all mappings in the map passed in.
    *
    * @param map      map containing mappings to enter
    * @param lifespan lifespan of the entry.  Negative values are interpreted as unlimited lifespan.
    * @param unit     unit of measurement for the lifespan
    */
   void putAll(Map<? extends K, ? extends V> map, long lifespan, TimeUnit unit);

   /**
    * An overloaded form of {@link #replace(Object, Object)}, which takes in lifespan parameters.
    *
    * @param key      key to use
    * @param value    value to store
    * @param lifespan lifespan of the entry.  Negative values are interpreted as unlimited lifespan.
    * @param unit     unit of measurement for the lifespan
    * @return the value being replaced, or null if nothing is being replaced.
    */
   V replace(K key, V value, long lifespan, TimeUnit unit);

   /**
    * An overloaded form of {@link #replace(Object, Object, Object)}, which takes in lifespan parameters.
    *
    * @param key      key to use
    * @param oldValue value to replace
    * @param value    value to store
    * @param lifespan lifespan of the entry.  Negative values are interpreted as unlimited lifespan.
    * @param unit     unit of measurement for the lifespan
    * @return true if the value was replaced, false otherwise
    */
   boolean replace(K key, V oldValue, V value, long lifespan, TimeUnit unit);

   /**
    * An overloaded form of {@link #put(Object, Object)}, which takes in lifespan parameters.
    *
    * @param key             key to use
    * @param value           value to store
    * @param lifespan        lifespan of the entry.  Negative values are interpreted as unlimited lifespan.
    * @param lifespanUnit    time unit for lifespan
    * @param maxIdleTime     the maximum amount of time this key is allowed to be idle for before it is considered as
    *                        expired
    * @param maxIdleTimeUnit time unit for max idle time
    * @return the value being replaced, or null if nothing is being replaced.
    */
   V put(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit);

   /**
    * An overloaded form of {@link #putIfAbsent(Object, Object)}, which takes in lifespan parameters.
    *
    * @param key             key to use
    * @param value           value to store
    * @param lifespan        lifespan of the entry.  Negative values are interpreted as unlimited lifespan.
    * @param lifespanUnit    time unit for lifespan
    * @param maxIdleTime     the maximum amount of time this key is allowed to be idle for before it is considered as
    *                        expired
    * @param maxIdleTimeUnit time unit for max idle time
    * @return the value being replaced, or null if nothing is being replaced.
    */
   V putIfAbsent(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit);

   /**
    * An overloaded form of {@link #putAll(Map)}, which takes in lifespan parameters.  Note that the lifespan is applied
    * to all mappings in the map passed in.
    *
    * @param map             map containing mappings to enter
    * @param lifespan        lifespan of the entry.  Negative values are interpreted as unlimited lifespan.
    * @param lifespanUnit    time unit for lifespan
    * @param maxIdleTime     the maximum amount of time this key is allowed to be idle for before it is considered as
    *                        expired
    * @param maxIdleTimeUnit time unit for max idle time
    */
   void putAll(Map<? extends K, ? extends V> map, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit);

   /**
    * An overloaded form of {@link #replace(Object, Object)}, which takes in lifespan parameters.
    *
    * @param key             key to use
    * @param value           value to store
    * @param lifespan        lifespan of the entry.  Negative values are interpreted as unlimited lifespan.
    * @param lifespanUnit    time unit for lifespan
    * @param maxIdleTime     the maximum amount of time this key is allowed to be idle for before it is considered as
    *                        expired
    * @param maxIdleTimeUnit time unit for max idle time
    * @return the value being replaced, or null if nothing is being replaced.
    */
   V replace(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit);

   /**
    * An overloaded form of {@link #replace(Object, Object, Object)}, which takes in lifespan parameters.
    *
    * @param key             key to use
    * @param oldValue        value to replace
    * @param value           value to store
    * @param lifespan        lifespan of the entry.  Negative values are interpreted as unlimited lifespan.
    * @param lifespanUnit    time unit for lifespan
    * @param maxIdleTime     the maximum amount of time this key is allowed to be idle for before it is considered as
    *                        expired
    * @param maxIdleTimeUnit time unit for max idle time
    * @return true if the value was replaced, false otherwise
    */
   boolean replace(K key, V oldValue, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit);

   /**
    * {@inheritDoc}
    *
    * If the return value of this operation will be ignored by the application,
    * the user is strongly encouraged to use the {@link org.infinispan.context.Flag#IGNORE_RETURN_VALUES}
    * flag when invoking this method in order to make it behave as efficiently
    * as possible (i.e. avoiding needless remote or network calls).
    */
   @Override
   V remove(Object key);

   /**
    * Retrieves a map of key-value mappings at once. The key set must not
    * be modified during execution of this method.
    *
    * @param keys
    * @return
    */
   Map<K, V> getMany(Set<K> keys);
}
