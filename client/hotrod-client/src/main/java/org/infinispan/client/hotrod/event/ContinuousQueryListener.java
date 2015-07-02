package org.infinispan.client.hotrod.event;

/**
 * @author anistor@redhat.com
 * @since 8.0
 */
public interface ContinuousQueryListener<K, V> {

   /**
    * Receives notification that a cache entry has joined the matching set.
    *
    * @param key
    * @param value
    */
   void resultJoining(K key, V value);

   /**
    * Receives notification that a cache entry has left the matching set.
    *
    * @param key
    */
   void resultLeaving(K key);
}
