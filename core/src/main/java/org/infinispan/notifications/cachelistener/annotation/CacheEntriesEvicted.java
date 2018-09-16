package org.infinispan.notifications.cachelistener.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This annotation should be used on methods that need to be notified when cache entries are evicted.
 * <p>
 * Methods annotated with this annotation should be public and take in a single parameter, a {@link
 * org.infinispan.notifications.cachelistener.event.CacheEntriesEvictedEvent} otherwise an {@link
 * org.infinispan.notifications.IncorrectListenerException} will be thrown when registering your cache listener.
 * <p>
 * Locking: notification is performed WITH locks on the given key.
 *
 * @author Manik Surtani
 * @author Galder Zamarreño
 * @see org.infinispan.notifications.Listener
 * @see CacheEntryLoaded
 * @since 5.0
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface CacheEntriesEvicted {
}
