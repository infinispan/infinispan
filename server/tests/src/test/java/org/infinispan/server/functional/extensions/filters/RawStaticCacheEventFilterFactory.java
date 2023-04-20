package org.infinispan.server.functional.extensions.filters;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;

import org.infinispan.filter.NamedFactory;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilter;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilterFactory;
import org.infinispan.notifications.cachelistener.filter.EventType;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoName;

@NamedFactory(name = "raw-static-filter-factory")
public class RawStaticCacheEventFilterFactory implements CacheEventFilterFactory {
   @Override
   public CacheEventFilter<byte[], byte[]> getFilter(Object[] params) {
      try {
         // Static key is 2 marshalled
         byte[] staticKey = ProtobufUtil.toWrappedByteArray(ProtobufUtil.newSerializationContext(), 2);
         return new RawStaticCacheEventFilter(staticKey);
      } catch (IOException e) {
         throw new IllegalStateException(e);
      }
   }

   @ProtoName("RawStaticCacheEventFilter")
   public static class RawStaticCacheEventFilter implements CacheEventFilter<byte[], byte[]>, Serializable {

      @ProtoField(1)
      final byte[] staticKey;

      @ProtoFactory
      RawStaticCacheEventFilter(byte[] staticKey) {
         this.staticKey = staticKey;
      }

      @Override
      public boolean accept(byte[] key, byte[] previousValue, Metadata previousMetadata, byte[] value,
                            Metadata metadata, EventType eventType) {
         return Arrays.equals(key, staticKey);
      }
   }
}
