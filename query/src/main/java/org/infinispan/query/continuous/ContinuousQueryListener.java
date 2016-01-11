package org.infinispan.query.continuous;

/**
 * Listener for continuous query events.
 *
 * @author anistor@redhat.com
 * @since 8.1
 */
public interface ContinuousQueryListener<K, V> {

   /**
    * Receives notification that a cache entry has joined the matching set.
    *
    * @param key the key of the joining entry
    * @param value the joining entry or the Object[] projection if specified
    */
   void resultJoining(K key, V value);

   /**
    * Receives notification that a cache entry has left the matching set.
    *
    * @param key the key of the leaving entry
    */
   void resultLeaving(K key);
}
