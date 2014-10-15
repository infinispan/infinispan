package org.infinispan.notifications.cachelistener;

import org.infinispan.filter.Converter;
import org.infinispan.filter.KeyValueFilter;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverter;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilter;
import org.infinispan.notifications.impl.ListenerInvocation;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.notifications.cachelistener.event.Event;

import java.util.UUID;

/**
 * Additional listener methods specific to caches.
 *
 * @author wburns
 * @since 7.0
 */
public interface CacheEntryListenerInvocation<K, V> extends ListenerInvocation<Event<K, V>> {
   void invoke(CacheEntryEvent<K, V> event, boolean isLocalNodePrimaryOwner);

   void invokeNoChecks(CacheEntryEvent<K, V> event, boolean skipQueue, boolean skipConverter);

   boolean isClustered();

   UUID getIdentifier();

   CacheEventFilter<? super K, ? super V> getFilter();

   <C> CacheEventConverter<? super K, ? super V, C> getConverter();
}
