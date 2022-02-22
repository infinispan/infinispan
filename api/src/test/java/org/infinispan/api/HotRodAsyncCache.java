package org.infinispan.api;

import org.infinispan.api.async.AsyncCache;

/**
 * @since 14.0
 **/
public interface HotRodAsyncCache<K, V> extends AsyncCache<K, V> {

   static <K1, V1> HotRodAsyncCache<K1, V1> unwrap(AsyncCache<K1, V1> cache) {
      return null;
   }
}
