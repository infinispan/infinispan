package org.infinispan.notifications.cachelistener;

import java.lang.annotation.Annotation;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

import org.infinispan.encoding.DataConversion;
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
   /**
    * Invokes the event
    * @param event
    * @param isLocalNodePrimaryOwner
    * @return null if event was ignored or already complete otherwise all listeners for the event will be notified when
    * the provided stage is completed
    */
   CompletionStage<Void> invoke(EventWrapper<K, V, CacheEntryEvent<K, V>> event, boolean isLocalNodePrimaryOwner);

   /**
    * Invokes the event without applying filters or converters
    * @param wrappedEvent
    * @param skipQueue
    * @param skipConverter
    * @param needsTransform
    * @return null if event was ignored or already complete otherwise all listeners for the event will be notified when
    * the provided stage is completed
    */
   CompletionStage<Void> invokeNoChecks(EventWrapper<K, V, CacheEntryEvent<K, V>> wrappedEvent, boolean skipQueue, boolean skipConverter, boolean needsTransform);

   boolean isClustered();

   boolean isSync();

   UUID getIdentifier();

   Listener.Observation getObservation();

   Class<? extends Annotation> getAnnotation();

   CacheEventFilter<? super K, ? super V> getFilter();

   <C> CacheEventConverter<? super K, ? super V, C> getConverter();

   Set<Class<? extends Annotation>> getFilterAnnotations();

   DataConversion getKeyDataConversion();

   DataConversion getValueDataConversion();

   /**
    * @return true if the filter/converter should be done in the storage format
    */
   boolean useStorageFormat();

}
