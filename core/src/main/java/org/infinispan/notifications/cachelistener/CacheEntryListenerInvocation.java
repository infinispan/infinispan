package org.infinispan.notifications.cachelistener;

import java.lang.annotation.Annotation;
import java.util.Set;
import java.util.UUID;

import org.infinispan.commons.dataconversion.Encoder;
import org.infinispan.commons.dataconversion.Wrapper;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.notifications.cachelistener.event.Event;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverter;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilter;
import org.infinispan.notifications.impl.ListenerInvocation;

/**
 * Additional listener methods specific to caches.
 *
 * @author wburns
 * @since 7.0
 */
public interface CacheEntryListenerInvocation<K, V> extends ListenerInvocation<Event<K, V>> {
   void invoke(EventWrapper<K, V, CacheEntryEvent<K, V>> event, boolean isLocalNodePrimaryOwner);

   void invokeNoChecks(EventWrapper<K, V, CacheEntryEvent<K, V>> wrappedEvent, boolean skipQueue, boolean skipConverter);

   boolean isClustered();

   boolean isSync();

   UUID getIdentifier();

   Listener.Observation getObservation();

   Class<? extends Annotation> getAnnotation();

   CacheEventFilter<? super K, ? super V> getFilter();

   <C> CacheEventConverter<? super K, ? super V, C> getConverter();

   Set<Class<? extends Annotation>> getFilterAnnotations();

   Encoder getKeyEncoder();

   Encoder getValueEncoder();

   Wrapper getKeyWrapper();

   Wrapper getValueWrapper();
}
