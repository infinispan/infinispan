package org.infinispan.server.hotrod.event;

import org.infinispan.commons.util.KeyValueWithPrevious;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverter;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverterFactory;
import org.infinispan.filter.NamedFactory;

@NamedFactory(name = "key-value-with-previous-converter-factory")
public class KeyValueWithPreviousEventConverterFactory<K, V> implements CacheEventConverterFactory {
   private final KeyValueWithPreviousEventConverter<K, V> converter = new KeyValueWithPreviousEventConverter<K, V>();

   @Override
   public CacheEventConverter<K, V, KeyValueWithPrevious<K, V>> getConverter(Object[] params) {
      return converter;
   }
}
