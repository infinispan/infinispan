package org.infinispan.server.hotrod;

import org.infinispan.notifications.cachelistener.filter.CacheEventConverter;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverterFactory;

class KeyValueVersionConverterFactory implements CacheEventConverterFactory {
   private KeyValueVersionConverterFactory() {
   }

   public static KeyValueVersionConverterFactory SINGLETON = new KeyValueVersionConverterFactory();

   @Override
   public <K, V, C> CacheEventConverter<K, V, C> getConverter(Object[] params) {
      KeyValueVersionConverter converter;
      // If a parameter is sent, we consider we want the old value. Don't consider the value
      // Related to RemoteApplicationPublishedBridge where expiration and deletion events need the value
      // https://issues.jboss.org/browse/ISPN-9634
      if (params != null && params.length > 0) {
         converter = KeyValueVersionConverter.INCLUDING_OLD_VALUE_CONVERTER;
      } else {
         converter = KeyValueVersionConverter.EXCLUDING_OLD_VALUE_CONVERTER;
      }
      return (CacheEventConverter<K, V, C>) converter;
   }
}
