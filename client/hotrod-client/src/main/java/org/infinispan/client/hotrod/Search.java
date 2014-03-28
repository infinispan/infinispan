package org.infinispan.client.hotrod;

import org.infinispan.client.hotrod.impl.RemoteCacheImpl;
import org.infinispan.client.hotrod.impl.query.RemoteQueryFactory;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
public final class Search {

   private Search() {
   }

   public static QueryFactory<Query> getQueryFactory(RemoteCache cache) {
      if (cache == null) {
         throw new IllegalArgumentException("cache parameter cannot be null");
      }

      return new RemoteQueryFactory((RemoteCacheImpl) cache);
   }
}
