package org.infinispan.notifications.cachelistener;

import org.infinispan.filter.Converter;
import org.infinispan.filter.KeyValueFilter;
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

   boolean shouldInvoke(CacheEntryEvent<K, V> event, boolean isLocalNodePrimaryOwner);

   boolean isClustered();

   UUID getIdentifier();

   KeyValueFilter<? super K, ? super V> getFilter();

   <C> Converter<? super K, ? super V, C> getConverter();
}
