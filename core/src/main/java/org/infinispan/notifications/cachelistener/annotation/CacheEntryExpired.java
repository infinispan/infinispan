package org.infinispan.notifications.cachelistener.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This annotation should be used on methods that need to be notified when a cache entry is expired
 * <p>
 * Methods annotated with this annotation should be public and take in a single parameter, a {@link
 * org.infinispan.notifications.cachelistener.event.CacheEntryExpiredEvent} otherwise an {@link
 * org.infinispan.notifications.IncorrectListenerException} will be thrown when registering your cache listener.
 * <p>
 * Locking: there is no guarantee as to whether the lock is acquired for this key, however there is internal
 * guarantees to make sure these events are not out of order
 * <p>
 * It is possible yet highly unlikely to receive this event right after a remove event even though the value
 * was previously removed.  This can happen in the case when an expired entry in a store (not present in memory) is
 * found by the reaper thread and a remove occurs at the same time.
 *
 * @author William Burns
 * @see org.infinispan.notifications.Listener
 * @since 8.0
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface CacheEntryExpired {
}
