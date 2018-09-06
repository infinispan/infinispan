package org.infinispan.server.hotrod;

import org.infinispan.notifications.cachelistener.filter.CacheEventConverter;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverterFactory;

/**
 * A factory which returns only a {@link KeyOnlyFilterConverter} instance
 */
class KeyOnlyFilterConverterFactory implements CacheEventConverterFactory {

   public static final String NAME = "org.infinispan.keyonlyfilterconverter";

   private KeyOnlyFilterConverterFactory() {
   }

   public static KeyOnlyFilterConverterFactory SINGLETON = new KeyOnlyFilterConverterFactory();

   @Override
   public <K, V, C> CacheEventConverter<K, V, C> getConverter(Object[] params) {
      return (CacheEventConverter<K, V, C>) KeyOnlyFilterConverter.SINGLETON;
   }
}
