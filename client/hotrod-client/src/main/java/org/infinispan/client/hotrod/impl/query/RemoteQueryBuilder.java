package org.infinispan.client.hotrod.impl.query;

import org.infinispan.client.hotrod.impl.RemoteCacheImpl;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.impl.BaseQueryBuilder;
import org.infinispan.query.dsl.impl.QueryStringCreator;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
final class RemoteQueryBuilder extends BaseQueryBuilder {

   private static final Log log = LogFactory.getLog(RemoteQueryBuilder.class);
   private static final boolean trace = log.isTraceEnabled();

   private final RemoteCacheImpl<?, ?> cache;
   private final SerializationContext serializationContext;

   RemoteQueryBuilder(RemoteQueryFactory queryFactory, RemoteCacheImpl<?, ?> cache, SerializationContext serializationContext, String rootType) {
      super(queryFactory, rootType);
      this.cache = cache;
      this.serializationContext = serializationContext;
   }

   @Override
   public Query build() {
      QueryStringCreator generator = serializationContext != null ?
            new RemoteQueryStringCreator(serializationContext) : new QueryStringCreator();
      String queryString = accept(generator);
      if (trace) {
         log.tracef("Query string : %s", queryString);
      }
      return new RemoteQuery(queryFactory, cache, serializationContext, queryString, generator.getNamedParameters(), getProjectionPaths(), startOffset, maxResults);
   }
}
