package org.infinispan.api.common.events.cache;

/**
 * @since 14.0
 */
public interface CacheContinuousQueryEvent<K, V> { // introduce subinterfaces

   EventType type();

   K key();

   V value();

   enum EventType { // Drop the enum and
      JOIN,
      UPDATE,
      LEAVE
   }
}
