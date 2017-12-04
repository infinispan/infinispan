package org.infinispan.jcache;

import javax.cache.Cache;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.EventType;

import org.infinispan.commons.util.ReflectionUtil;

/**
 * The reference implementation of the {@link CacheEntryEvent}.
 *
 * @param <K> the type of keys maintained by this cache
 * @param <V> the type of cached values
 * @author Greg Luck
 * @since 1.0
 */
public class RICacheEntryEvent<K, V> extends CacheEntryEvent<K, V> {

    /** The serialVersionUID */
    private static final long serialVersionUID = 6515030413069752679L;

    private K key;
    private V value;
    private V oldValue;
    private boolean oldValueAvailable;

   /**
    * Constructs a cache entry event from a given cache as source
    * (without an old value)
    *
    * @param source the cache that originated the event
    * @param key    the key
    * @param value  the value
    */
   public RICacheEntryEvent(Cache<K, V> source, K key, V value, EventType eventType) {
      super(source, eventType);
      this.key = key;
      this.value = value;
      this.oldValue = null;
      this.oldValueAvailable = false;
   }

   /**
    * Constructs a cache entry event from a given cache as source
    * (with an old value)
    *
    * @param source   the cache that originated the event
    * @param key      the key
    * @param value    the value
    * @param oldValue the oldValue
    */
   public RICacheEntryEvent(Cache<K, V> source, K key, V value, V oldValue, EventType eventType) {
      super(source, eventType);
      this.key = key;
      this.value = value;
      this.oldValue = oldValue;
      this.oldValueAvailable = true;
   }

   /**
    * Constructs a cache entry event from a given cache as source
    * with an old value, explicitly specifying whether old value is available
    *
    * @param source            the cache that originated the event
    * @param key               the key
    * @param value             the value
    * @param oldValue          the oldValue
    * @param oldValueAvailable indicates whether old value is available
    */
   public RICacheEntryEvent(Cache<K, V> source, K key, V value, V oldValue, EventType eventType, boolean oldValueAvailable) {
      super(source, eventType);
      this.key = key;
      this.value = value;
      this.oldValue = oldValue;
      this.oldValueAvailable = oldValueAvailable;
   }

    /**
     * Returns the key of the cache entry with the event
     *
     * @return the key
     */
    @Override
    public K getKey() {
        return key;
    }

    /**
     * Returns the value of the cache entry with the event
     *
     * @return the value
     */
    @Override
    public V getValue() {
        return value;
    }

   /**
    * {@inheritDoc}
    */
   @Override
   public <T> T unwrap(Class<T> clazz) {
      return ReflectionUtil.unwrap(this, clazz);
   }

   /**
    * {@inheritDoc}
    */
    @Override
    public V getOldValue() {
        if (isOldValueAvailable()) {
            return oldValue;
        } else {
            return null;
        }
    }

    /**
     * Whether the old value is available
     *
     * @return true if the old value is populated
     */
    @Override
    public boolean isOldValueAvailable() {
        return oldValueAvailable;
    }
}
