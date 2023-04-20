package org.infinispan.server.functional.extensions.filters;

import java.io.Serializable;

import org.infinispan.filter.NamedFactory;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilter;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilterFactory;
import org.infinispan.notifications.cachelistener.filter.EventType;
import org.infinispan.protostream.WrappedMessage;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;

@NamedFactory(name = "static-filter-factory")
public class StaticCacheEventFilterFactory<K> implements CacheEventFilterFactory {

   @Override
   public CacheEventFilter<K, String> getFilter(Object[] params) {
      return new StaticCacheEventFilter<>((K) params[0]);
   }

   public static class StaticCacheEventFilter<K> implements CacheEventFilter<K, String>, Serializable {
      final K staticKey;

      StaticCacheEventFilter(K staticKey) {
         this.staticKey = staticKey;
      }

      @ProtoFactory
      StaticCacheEventFilter(WrappedMessage staticKey) {
         this.staticKey = (K) staticKey.getValue();
      }

      @ProtoField(1)
      public WrappedMessage getStaticKey() {
         return new WrappedMessage(staticKey);
      }

      @Override
      public boolean accept(K key, String previousValue, Metadata previousMetadata, String value,
                            Metadata metadata, EventType eventType) {
         return staticKey.equals(key);
      }
   }
}
