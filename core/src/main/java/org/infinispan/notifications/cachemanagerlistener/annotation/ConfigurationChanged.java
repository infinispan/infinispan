package org.infinispan.notifications.cachemanagerlistener.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This annotation should be used on methods that need to be notified when a configuration is changed.
 * Methods annotated with this annotation should accept a single parameter, a {@link
 * org.infinispan.notifications.cachemanagerlistener.event.ConfigurationChangedEvent} otherwise a {@link
 * org.infinispan.notifications.IncorrectListenerException} will be thrown when registering your listener.
 * Any exceptions thrown by the listener will abort the call. Any other listeners not yet called will not be called,
 * and any transactions in progress will be rolled back.
 *
 * @author Tristan Tarrant
 * @see org.infinispan.notifications.Listener
 * @since 13.0
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface ConfigurationChanged {
}
