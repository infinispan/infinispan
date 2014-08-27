package org.infinispan.client.hotrod.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation that marks a class to receive remote events from Hot Rod caches.
 * Classes with this annotation are expected to have at least one callback
 * annotated with one of the events it can receive:
 * {@link org.infinispan.client.hotrod.annotation.ClientCacheEntryCreated},
 * {@link org.infinispan.client.hotrod.annotation.ClientCacheEntryModified},
 * {@link org.infinispan.client.hotrod.annotation.ClientCacheEntryRemoved},
 * {@link org.infinispan.client.hotrod.annotation.ClientCacheFailover}
 *
 * @author Galder Zamarreño
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface ClientListener {

   /**
    * Defines the key/value filter factory for this client listener. Filter
    * factories create filters that help decide which events should be sent
    * to this client listener. This helps with reducing traffic from server
    * to client. By default, no filtering is applied.
    */
   String filterFactoryName() default "";

   /**
    * Defines the converter factory for this client listener. Converter
    * factories create event converters that customize the contents of the
    * events sent to this listener. When event customization is enabled,
    * {@link org.infinispan.client.hotrod.annotation.ClientCacheEntryCreated},
    * {@link org.infinispan.client.hotrod.annotation.ClientCacheEntryModified},
    * and {@link org.infinispan.client.hotrod.annotation.ClientCacheEntryRemoved}
    * callbacks receive {@link org.infinispan.client.hotrod.event.ClientCacheEntryCustomEvent}
    * instances as parameters instead of their corresponding create/modified/removed
    * event. Event customization helps reduce the payload of events, or
    * increase to send even more information back to the client listener.
    * By default, no event customization is applied.
    */
   String converterFactoryName() default "";

   /**
    * This flag enables cached state to be sent back to remote clients when
    * either adding a cache listener for the first time, or when the node where
    * a remote listener is registered changes. When enabled, state is sent
    * back as cache entry created events to the clients. In the special case
    * that the node where the remote listener is registered changes, before
    * sending any cache entry created events, the client receives a failover
    * event so that it's aware of the change of node. This is useful in order
    * to do local clean up before receiving the state again. For example, a
    * client building a local near cache and keeping it up to date with remote
    * events might decide to clear in when the failover event is received and
    * before the state is received.
    *
    * If disabled, no state is sent back to the client when adding a listener,
    * nor it gets state when the node where the listener is registered changes.
    *
    * By default, including state is disabled in order to provide best
    * performance. If clients must receive all events, enable including state.
    */
   boolean includeCurrentState() default false;

}
