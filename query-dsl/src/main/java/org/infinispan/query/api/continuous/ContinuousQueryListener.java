package org.infinispan.query.api.continuous;

/**
 * Listener for continuous query events.
 *
 * @author anistor@redhat.com
 * @since 8.2
 */
public interface ContinuousQueryListener<K, V> {

   /**
    * Receives notification that a cache entry has joined the matching set. This is invoked initially when receiving the
    * existing entries that match the query and subsequently whenever a previously non-matching entry is updated and
    * starts to match.
    *
    * @param key   the key of the joining entry
    * @param value the joining entry or the Object[] projection if a projection was specified
    */
   default void resultJoining(K key, V value) {
   }

   /**
    * Receives notification that a cache entry from the matching set was updated and continues to match the query. The
    * modified attributes causing this update are not necessarily part of the query.
    *
    * @param key   the key of the joining entry
    * @param value the joining entry or the Object[] projection if specified
    */
   default void resultUpdated(K key, V value) {
   }

   /**
    * Receives notification that a cache entry has left the matching set. This can happen due to an update or removal.
    *
    * @param key the key of the leaving entry
    */
   default void resultLeaving(K key) {
   }
}
