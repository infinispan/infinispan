package org.infinispan.notifications.cachelistener.filter;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.filter.KeyValueFilter;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.event.Event;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * KeyValueFilter that is implemented by using the provided CacheEventFilter.  The provided event type will always be
 * one that is not retried, post and of type CREATE,  The old value and old metadata in both pre and post events will
 * be the data that was in the cache before the event occurs.  The new value and new metadata in both pre and post
 * events will be the data that is in the cache after the event occurs.
 *
 * @author wburns
 * @since 7.0
 */
@ProtoTypeId(ProtoStreamTypeIds.CACHE_EVENT_FILTER_AS_KEY_VALUE_FILTER)
@Scope(Scopes.NONE)
public class CacheEventFilterAsKeyValueFilter<K, V> implements KeyValueFilter<K, V> {
   private static final EventType CREATE_EVENT = new EventType(false, false, Event.Type.CACHE_ENTRY_CREATED);

   private final CacheEventFilter<K, V> filter;

   public CacheEventFilterAsKeyValueFilter(CacheEventFilter<K, V> filter) {
      this.filter = filter;
   }

   @ProtoFactory
   CacheEventFilterAsKeyValueFilter(MarshallableObject<CacheEventFilter<K, V>> filter) {
      this.filter = MarshallableObject.unwrap(filter);
   }

   @ProtoField(1)
   MarshallableObject<CacheEventFilter<K, V>> getFilter() {
      return MarshallableObject.create(filter);
   }

   @Override
   public boolean accept(K key, V value, Metadata metadata) {
      return filter.accept(key, null, null, value, metadata, CREATE_EVENT);
   }

   @Inject
   protected void injectDependencies(ComponentRegistry cr) {
      cr.wireDependencies(filter);
   }
}
