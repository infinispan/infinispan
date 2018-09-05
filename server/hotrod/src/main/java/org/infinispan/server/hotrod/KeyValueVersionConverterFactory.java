package org.infinispan.server.hotrod;

import org.infinispan.notifications.cachelistener.filter.CacheEventConverter;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverterFactory;

class KeyValueVersionConverterFactory implements CacheEventConverterFactory {

   public static final String NAME = "___eager-key-value-version-converter";

   private KeyValueVersionConverterFactory() {
   }

   public static KeyValueVersionConverterFactory SINGLETON = new KeyValueVersionConverterFactory();

   @Override
   public <K, V, C> CacheEventConverter<K, V, C> getConverter(Object[] params) {
      return (CacheEventConverter<K, V, C>) KeyValueVersionConverter.SINGLETON; // ugly but it works :|
   }
}
