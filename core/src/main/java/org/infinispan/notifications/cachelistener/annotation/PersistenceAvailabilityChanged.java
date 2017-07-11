package org.infinispan.notifications.cachelistener.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This annotation should be used on methods that need to be notified when the availability of the PersistenceManager
 * changes. When Cache stores are configured, but the connection to at least one store is lost, the PersistenceManager becomes
 * unavailable. This results in a {@link org.infinispan.persistence.spi.StoreUnavailableException} being thrown on all read/write
 * operations which require the PersistenceManager until all stores once again become available.
 * <p/>
 * Methods annotated with this annotation should be public and take in a single parameter, a {@link
 * org.infinispan.notifications.cachelistener.event.CacheEntryActivatedEvent} otherwise an {@link
 * org.infinispan.notifications.IncorrectListenerException} will be thrown when registering your cache listener.
 * Locking: notification is performed WITH locks on the given key.
 * <p/>
 * Any exceptions thrown by the listener will abort the call. Any other listeners not yet called will not be called,
 * and any transactions in progress will be rolled back.
 *
 * @author Ryan Emerson
 * @see org.infinispan.notifications.Listener
 * @since 9.3
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface PersistenceAvailabilityChanged {
}
