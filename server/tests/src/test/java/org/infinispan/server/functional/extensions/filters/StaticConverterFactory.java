package org.infinispan.server.functional.extensions.filters;

import java.io.Serializable;

import org.infinispan.filter.NamedFactory;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverter;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverterFactory;
import org.infinispan.notifications.cachelistener.filter.EventType;
import org.infinispan.protostream.annotations.ProtoName;
import org.infinispan.server.functional.extensions.entities.Entities;

@NamedFactory(name = "static-converter-factory")
public class StaticConverterFactory<K> implements CacheEventConverterFactory {
   @Override
   public CacheEventConverter<K, String, Entities.CustomEvent<K>> getConverter(Object[] params) {
      return new StaticConverter<>();
   }

   @ProtoName("StaticConverter")
   public static class StaticConverter<K> implements CacheEventConverter<K, String, Entities.CustomEvent<K>>, Serializable {
      @Override
      public Entities.CustomEvent<K> convert(K key, String previousValue, Metadata previousMetadata, String value,
                                             Metadata metadata, EventType eventType) {
         return new Entities.CustomEvent<>(key, value, 0);
      }
   }
}
