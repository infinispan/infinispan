package org.infinispan.query.api.continuous;

/**
 * Listener for continuous query events.
 *
 * @author anistor@redhat.com
 * @since 8.2
 */
public interface ContinuousQueryListener<K, V> {

   /**
    * Receives notification that a cache entry has joined the matching set.
    *
    * @param key   the key of the joining entry
    * @param value the joining entry or the Object[] projection if specified
    */
   default void resultJoining(K key, V value) {
   }

   /**
    * Receives notification that a cache entry from the matching set was updated and continues to match the query.
    *
    * @param key   the key of the joining entry
    * @param value the joining entry or the Object[] projection if specified
    */
   default void resultUpdated(K key, V value) {
   }

   /**
    * Receives notification that a cache entry has left the matching set.
    *
    * @param key the key of the leaving entry
    */
   default void resultLeaving(K key) {
   }
}
