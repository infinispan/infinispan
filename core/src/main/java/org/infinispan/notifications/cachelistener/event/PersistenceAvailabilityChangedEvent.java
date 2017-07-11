package org.infinispan.notifications.cachelistener.event;

public interface PersistenceAvailabilityChangedEvent<K, V> extends Event<K, V> {
   /**
    * @return true if the {@link org.infinispan.persistence.manager.PersistenceManager} is available.
    */
   boolean isAvailable();
}
