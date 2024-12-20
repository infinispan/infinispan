package org.infinispan.notifications.cachelistener.filter;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.filter.Converter;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.event.Event;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Converter that is implemented by using the provided CacheEventConverter.  The provided event type will always be
 * one that is not retried, post and of type CREATE,  The old value and old metadata in both pre and post events will
 * be the data that was in the cache before the event occurs.  The new value and new metadata in both pre and post
 * events will be the data that is in the cache after the event occurs.
 *
 * @author wburns
 * @since 7.0
 */
@ProtoTypeId(ProtoStreamTypeIds.CACHE_EVENT_CONVERTER_AS_CONVERTER)
@Scope(Scopes.NONE)
public class CacheEventConverterAsConverter<K, V, C> implements Converter<K, V, C> {
   private static final EventType CREATE_EVENT = new EventType(false, false, Event.Type.CACHE_ENTRY_CREATED);

   private final CacheEventConverter<K, V, C> converter;

   public CacheEventConverterAsConverter(CacheEventConverter<K, V, C> converter) {
      this.converter = converter;
   }

   @ProtoFactory
   CacheEventConverterAsConverter(MarshallableObject<CacheEventConverter<K, V, C>> converter) {
      this.converter = MarshallableObject.unwrap(converter);
   }

   @ProtoField(number = 1)
   MarshallableObject<CacheEventConverter<K, V, C>> getConverter() {
      return MarshallableObject.create(converter);
   }

   @Override
   public C convert(K key, V value, Metadata metadata) {
      return converter.convert(key, null, null, value, metadata, CREATE_EVENT);
   }

   @Inject
   protected void injectDependencies(ComponentRegistry cr) {
      cr.wireDependencies(converter);
   }
}
