package org.infinispan.client.hotrod.impl.query;

import org.infinispan.client.hotrod.impl.RemoteCacheImpl;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryBuilder;
import org.infinispan.query.dsl.impl.BaseQueryFactory;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
public class RemoteQueryFactory extends BaseQueryFactory<Query> {

   private final RemoteCacheImpl cache;

   public RemoteQueryFactory(RemoteCacheImpl cache) {
      this.cache = cache;
   }

   @Override
   public QueryBuilder<Query> from(Class entityType) {
      return new RemoteQueryBuilder(cache, entityType);
   }
}
