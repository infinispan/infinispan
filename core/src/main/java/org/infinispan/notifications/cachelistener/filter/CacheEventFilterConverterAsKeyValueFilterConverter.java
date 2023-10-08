package org.infinispan.notifications.cachelistener.filter;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.filter.AbstractKeyValueFilterConverter;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.event.Event;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * @author anistor@redhat.com
 * @since 7.2
 */
@ProtoTypeId(ProtoStreamTypeIds.KEY_VALUE_FILTER_CONVERTER_AS_CACHE_EVENT_FILTER_CONVERTER)
@Scope(Scopes.NONE)
public class CacheEventFilterConverterAsKeyValueFilterConverter<K, V, C> extends AbstractKeyValueFilterConverter<K, V, C> {

   private static final EventType CREATE_EVENT = new EventType(false, false, Event.Type.CACHE_ENTRY_CREATED);

   private final CacheEventFilterConverter<K, V, C> cacheEventFilterConverter;

   public CacheEventFilterConverterAsKeyValueFilterConverter(CacheEventFilterConverter<K, V, C> cacheEventFilterConverter) {
      this.cacheEventFilterConverter = cacheEventFilterConverter;
   }

   @ProtoFactory
   CacheEventFilterConverterAsKeyValueFilterConverter(MarshallableObject<CacheEventFilterConverter<K, V, C>> converter) {
      this.cacheEventFilterConverter = MarshallableObject.unwrap(converter);
   }

   @ProtoField(1)
   MarshallableObject<CacheEventFilterConverter<K, V, C>> getConverter() {
      return MarshallableObject.create(cacheEventFilterConverter);
   }

   @Override
   public C filterAndConvert(K key, V value, Metadata metadata) {
      return cacheEventFilterConverter.filterAndConvert(key, null, null, value, metadata, CREATE_EVENT);
   }

   @Inject
   protected void injectDependencies(ComponentRegistry cr) {
      cr.wireDependencies(cacheEventFilterConverter);
   }
}
