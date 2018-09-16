package org.infinispan.notifications.cachelistener.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This annotation should be used on methods that need to be notified when a cache entry is activated.
 * <p>
 * Methods annotated with this annotation should be public and take in a single parameter, a {@link
 * org.infinispan.notifications.cachelistener.event.CacheEntryActivatedEvent} otherwise an {@link
 * org.infinispan.notifications.IncorrectListenerException} will be thrown when registering your cache listener.
 * <p>
 * Locking: notification is performed WITH locks on the given key.
 * <p>
 * Any exceptions thrown by the listener will abort the call. Any other listeners not yet called will not be called,
 * and any transactions in progress will be rolled back.
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani</a>
 * @see org.infinispan.notifications.Listener
 * @see CacheEntryPassivated
 * @since 4.0
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface CacheEntryActivated {
}
