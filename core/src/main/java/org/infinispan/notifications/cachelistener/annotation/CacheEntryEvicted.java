package org.infinispan.notifications.cachelistener.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This annotation should be used on methods that need to be notified when cache entries are evicted.
 * <p/>
 * Methods annotated with this annotation should be public and take in a single parameter, a {@link
 * org.infinispan.notifications.cachelistener.event.CacheEntryEvictedEvent} otherwise an {@link
 * org.infinispan.notifications.IncorrectListenerException} will be thrown when registering your cache listener.
 * <p/>
 * Any exceptions thrown by the listener will abort the call. Any other listeners not yet called will not be called,
 * and any transactions in progress will be rolled back.
 *
 * @author Manik Surtani
 * @author Galder Zamarreño
 * @see org.infinispan.notifications.Listener
 * @see CacheEntryLoaded
 * @deprecated Note that this annotation will be removed in Infinispan 6.0
 * @see CacheEntriesEvicted
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@Deprecated
public @interface CacheEntryEvicted {
}