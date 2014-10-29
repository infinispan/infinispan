package org.infinispan.notifications.cachelistener.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.infinispan.configuration.cache.CacheMode;

/**
 * This annotation should be used on methods that need to be notified when the
 * {@link org.infinispan.partionhandling.AvailabilityMode} in use by the
 * {@link org.infinispan.partionhandling.impl.PartitionHandlingManager} changes due to a change in cluster topology.
 * This is only fired in a {@link CacheMode#DIST_SYNC}, {@link CacheMode#DIST_ASYNC}, {@link CacheMode#REPL_SYNC} or
 * {@link CacheMode#REPL_ASYNC} configured cache.
 * <p/>
 * Methods annotated with this annotation should accept a single parameter, a
 * {@link org.infinispan.notifications.cachelistener.event.PartitionStatusChangedEvent} otherwise a
 * {@link org.infinispan.notifications.IncorrectListenerException} will be thrown when registering your listener.
 * <p/>
 * Note that methods marked with this annotation will be fired <i>before</i> and <i>after</i> the updated
 * {@link org.infinispan.partionhandling.AvailabilityMode}
 * is updated, i.e., your method will be called twice, with
 * {@link org.infinispan.notifications.cachelistener.event.Event#isPre()} being set to <tt>true</tt> as well
 * as <tt>false</tt>.
 * <p/>
 *
 * @author William Burns
 * @see org.infinispan.notifications.Listener
 * @since 7.0
 */
// ensure this annotation is available at runtime.
@Retention(RetentionPolicy.RUNTIME)
// ensure that this annotation is applied to classes.
@Target(ElementType.METHOD)
public @interface PartitionStatusChanged {
}
