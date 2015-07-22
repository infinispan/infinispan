package org.infinispan.notifications.cachelistener.event;

/**
 * This event subtype is passed in to any method annotated with
 * {@link org.infinispan.notifications.cachelistener.annotation.CacheEntryExpired}.
 * <p />
 * The {@link #getValue()} method returns the value of the entry before it expired.  Note this value may be null if
 * the entry expired from a cache store
 * <p>
 * This is a post only event
 *
 * @author William Burns
 * @since 8.0
 */
public interface CacheEntryExpiredEvent<K, V> extends CacheEntryEvent<K, V> {

   /**
    * Retrieves the value of the entry being expired.  Note this event is raised after the value has been expired.
    * <p />
    * @return the value of the entry expired
    */
   @Override
   V getValue();
}
