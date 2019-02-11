package org.infinispan.client.hotrod;

import org.infinispan.client.hotrod.event.impl.ContinuousQueryImpl;
import org.infinispan.client.hotrod.impl.RemoteCacheImpl;
import org.infinispan.client.hotrod.impl.query.RemoteQueryFactory;
import org.infinispan.query.api.continuous.ContinuousQuery;
import org.infinispan.query.dsl.QueryFactory;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
public final class Search {

   private Search() {
   }

   public static QueryFactory getQueryFactory(RemoteCache<?, ?> cache) {
      if (cache == null) {
         throw new IllegalArgumentException("cache parameter cannot be null");
      }

      return new RemoteQueryFactory((RemoteCacheImpl) cache);
   }

   public static <K, V> ContinuousQuery<K, V> getContinuousQuery(RemoteCache<K, V> cache) {
      return new ContinuousQueryImpl<K, V>(cache);
   }
}
