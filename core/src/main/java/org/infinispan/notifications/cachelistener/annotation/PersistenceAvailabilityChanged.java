package org.infinispan.notifications.cachelistener.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Use this annotation on methods that require notification when the availability of the PersistenceManager changes.
 * When Cache stores are configured, but the connection to at least one store is lost, the PersistenceManager becomes
 * unavailable. As a result, {@link org.infinispan.persistence.spi.StoreUnavailableException} is thrown on all read/write
 * operations that require the PersistenceManager until all stores become available again.
 * <p/>
 * Methods that use this annotation should be public and take one parameter, {@link
 * org.infinispan.notifications.cachelistener.event.CacheEntryActivatedEvent}. Otherwise {@link
 * org.infinispan.notifications.IncorrectListenerException} is thrown when registering your cache listener.
 * Locking: notification is performed WITH locks on the given key.
 * <p/>
 * If the listener throws any exceptions, the call aborts. No other listeners are called. Any transactions in progress
 * are rolled back.
 *
 * @author Ryan Emerson
 * @see org.infinispan.notifications.Listener
 * @since 9.3
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface PersistenceAvailabilityChanged {
}
