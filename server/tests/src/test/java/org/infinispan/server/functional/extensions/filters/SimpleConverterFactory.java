package org.infinispan.server.functional.extensions.filters;

import java.io.Serializable;

import org.infinispan.filter.NamedFactory;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverter;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverterFactory;
import org.infinispan.notifications.cachelistener.filter.EventType;
import org.infinispan.protostream.annotations.ProtoName;
import org.infinispan.server.functional.extensions.entities.Entities;

@NamedFactory(name = "simple-converter-factory")
public class SimpleConverterFactory<K> implements CacheEventConverterFactory {
   @Override
   @SuppressWarnings("unchecked")
   public CacheEventConverter<String, String, Entities.CustomEvent<String>> getConverter(Object[] params) {
      return new SimpleConverter();
   }

   @ProtoName("SimpleConverter")
   public static class SimpleConverter<K> implements CacheEventConverter<K, String, String>, Serializable {
      @Override
      public String convert(K key, String oldValue, Metadata oldMetadata, String newValue, Metadata newMetadata, EventType eventType) {
         if (newValue != null) return newValue;
         return oldValue;
      }
   }
}
