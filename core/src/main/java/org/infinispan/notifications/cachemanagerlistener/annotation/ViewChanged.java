package org.infinispan.notifications.cachemanagerlistener.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This annotation should be used on methods that need to be notified when the cache is used in a cluster and the
 * cluster topology changes (i.e., a member joins or leaves the cluster).
  * Methods annotated with this annotation should accept a single parameter, a {@link
 * org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent} otherwise a {@link
 * org.infinispan.notifications.IncorrectListenerException} will be thrown when registering your listener.
  * Any exceptions thrown by the listener will abort the call. Any other listeners not yet called will not be called,
 * and any transactions in progress will be rolled back.
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani</a>
 * @see org.infinispan.notifications.Listener
 * @since 4.0
 */
// ensure this annotation is available at runtime.
@Retention(RetentionPolicy.RUNTIME)
// ensure that this annotation is applied to classes.
@Target(ElementType.METHOD)
public @interface ViewChanged {
}
