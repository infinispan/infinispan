package org.infinispan.notifications.cachelistener.filter;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.filter.KeyValueFilterConverter;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.metadata.Metadata;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * {@link CacheEventFilterConverter} that uses an underlying {@link KeyValueFilterConverter} to do the conversion and
 * filtering. The new value and metadata are used as arguments to the underlying filter converter as it doesn't take
 * both new and old. The old value is not returned in any event.
 * @author wburns
 * @since 9.4
 */
@ProtoTypeId(ProtoStreamTypeIds.KEY_VALUE_FILTER_CONVERTER_AS_CACHE_KEY_VALUE_CONVERTER)
@Scope(Scopes.NONE)
public class KeyValueFilterConverterAsCacheEventFilterConverter<K, V, C> implements CacheEventFilterConverter<K, V, C> {
   private final KeyValueFilterConverter<K, V, C> keyValueFilterConverter;
   private final MediaType format;

   public KeyValueFilterConverterAsCacheEventFilterConverter(KeyValueFilterConverter<K, V, C> keyValueFilterConverter) {
      this(keyValueFilterConverter, MediaType.APPLICATION_OBJECT);
   }

   public KeyValueFilterConverterAsCacheEventFilterConverter(KeyValueFilterConverter<K, V, C> keyValueFilterConverter, MediaType format) {
      this.keyValueFilterConverter = keyValueFilterConverter;
      // If the format is unknown, defaults to use the storage type.
      this.format = format == MediaType.APPLICATION_UNKNOWN ? null : format;
   }


   @ProtoFactory
   KeyValueFilterConverterAsCacheEventFilterConverter(MarshallableObject<KeyValueFilterConverter<K, V, C>> converter, MediaType format) {
      this(MarshallableObject.unwrap(converter), format);
   }

   @ProtoField(1)
   MarshallableObject<KeyValueFilterConverter<K, V, C>> getConverter() {
      return MarshallableObject.create(keyValueFilterConverter);
   }

   @ProtoField(2)
   public MediaType format() {
      return format;
   }

   @Override
   public C filterAndConvert(K key, V oldValue, Metadata oldMetadata, V newValue, Metadata newMetadata, EventType eventType) {
      return keyValueFilterConverter.convert(key, newValue, newMetadata);
   }

   @Override
   public C convert(K key, V oldValue, Metadata oldMetadata, V newValue, Metadata newMetadata, EventType eventType) {
      return keyValueFilterConverter.convert(key, newValue, newMetadata);
   }

   @Override
   public boolean accept(K key, V oldValue, Metadata oldMetadata, V newValue, Metadata newMetadata, EventType eventType) {
      return keyValueFilterConverter.accept(key, newValue, newMetadata);
   }

   @Override
   public boolean includeOldValue() {
      // No reason to include old if new value is only ever used for conversions
      return false;
   }

   @Inject
   protected void injectDependencies(ComponentRegistry cr) {
      cr.wireDependencies(keyValueFilterConverter);
   }
}
