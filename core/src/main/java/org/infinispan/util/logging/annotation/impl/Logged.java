package org.infinispan.util.logging.annotation.impl;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * <b>This annotation is for internal use only!</b>
 * <p/>
 * This annotation should be used on methods that need to be notified when information is logged by the
 * {@link org.infinispan.util.logging.events.EventLogger}. There is no distinction between the log level or category.
 * <p/>
 * Methods annotated with this annotation should accept a single parameter, an {@link
 * org.infinispan.util.logging.events.EventLog}, otherwise a {@link
 * org.infinispan.notifications.IncorrectListenerException} will be thrown when registering your listener.
 * <p/>
 * Any exceptions thrown by the listener will abort the call. Any other listeners not yet called will not be called.
 *
 * @see org.infinispan.notifications.Listener
 * @since 14.0
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Logged {
}
