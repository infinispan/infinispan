package org.infinispan.server.hotrod.event;

import org.infinispan.commons.event.KVPEvent;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverter;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverterFactory;
import org.infinispan.notifications.cachelistener.filter.NamedFactory;

@NamedFactory(name = "kvp-converter-factory")
public class KVPEventConverterFactory<K, V> implements CacheEventConverterFactory {
   private final KVPEventConverter<K, V> converter = new KVPEventConverter<K, V>();

   @Override
   public CacheEventConverter<K, V, KVPEvent<K, V>> getConverter(Object[] params) {
      return converter;
   }
}
