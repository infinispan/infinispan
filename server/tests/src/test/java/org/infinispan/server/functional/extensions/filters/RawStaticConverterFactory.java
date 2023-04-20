package org.infinispan.server.functional.extensions.filters;

import java.io.Serializable;

import org.infinispan.commons.util.Util;
import org.infinispan.filter.NamedFactory;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverter;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverterFactory;
import org.infinispan.notifications.cachelistener.filter.EventType;
import org.infinispan.protostream.annotations.ProtoName;

@NamedFactory(name = "raw-static-converter-factory")
public class RawStaticConverterFactory implements CacheEventConverterFactory {
   @Override
   public CacheEventConverter<byte[], byte[], byte[]> getConverter(Object[] params) {
      return new RawStaticConverter();
   }

   @ProtoName("RawStaticConverter")
   public static class RawStaticConverter implements CacheEventConverter<byte[], byte[], byte[]>, Serializable {
      @Override
      public byte[] convert(byte[] key, byte[] previousValue, Metadata previousMetadata, byte[] value,
                            Metadata metadata, EventType eventType) {
         return value != null ? Util.concat(key, value) : key;
      }
   }
}
