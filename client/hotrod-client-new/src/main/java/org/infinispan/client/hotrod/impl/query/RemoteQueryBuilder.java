package org.infinispan.client.hotrod.impl.query;

import org.infinispan.client.hotrod.impl.InternalRemoteCache;
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

   private final InternalRemoteCache<?, ?> cache;
   private final SerializationContext serializationContext;

   RemoteQueryBuilder(RemoteQueryFactory queryFactory, InternalRemoteCache<?, ?> cache, SerializationContext serializationContext, String rootType) {
      super(queryFactory, rootType);
      this.cache = cache;
      this.serializationContext = serializationContext;
   }

   @Override
   public <T> Query<T> build() {
      QueryStringCreator generator = serializationContext != null ?
            new RemoteQueryStringCreator(serializationContext) : new QueryStringCreator();
      String queryString = accept(generator);
      if (log.isTraceEnabled()) {
         log.tracef("Query string : %s", queryString);
      }
      // QueryBuilder is deprecated and will not support local mode
      return new RemoteQuery<>(queryFactory, cache, serializationContext, queryString, generator.getNamedParameters(), getProjectionPaths(), startOffset, maxResults, false);
   }
}
