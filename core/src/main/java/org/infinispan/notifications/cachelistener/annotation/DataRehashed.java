package org.infinispan.notifications.cachelistener.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.notifications.IncorrectListenerException;
import org.infinispan.notifications.cachelistener.event.DataRehashedEvent;
import org.infinispan.notifications.cachelistener.event.Event;

/**
 * This annotation should be used on methods that need to be notified when a rehash starts or ends.
 * A "rehash" (or "rebalance") is the interval during which nodes are transferring data between each
 * other. When the event with pre = false is fired all nodes have received all data; Some nodes can
 * still keep old data, though - old data cleanup is executed after this event is fired.
 * <p/>
 * Methods annotated with this annotation should accept a single parameter, a {@link DataRehashedEvent} otherwise a
 * {@link IncorrectListenerException} will be thrown when registering your listener.
 * <p/>
 * Note that methods marked with this annotation will be fired <i>before</i> and <i>after</i> rehashing takes place,
 * i.e., your method will be called twice, with {@link Event#isPre()} being set to <tt>true</tt> as well as <tt>false</tt>.
 * <p/>
 * Any exceptions thrown by the listener will abort the call. Any other listeners not yet called will not be called,
 * and any transactions in progress will be rolled back.
 *
 * @author Manik Surtani
 * @see org.infinispan.notifications.Listener
 * @since 5.0
 */
// ensure this annotation is available at runtime.
@Retention(RetentionPolicy.RUNTIME)
// ensure that this annotation is applied to classes.
@Target(ElementType.METHOD)
public @interface DataRehashed {
}
