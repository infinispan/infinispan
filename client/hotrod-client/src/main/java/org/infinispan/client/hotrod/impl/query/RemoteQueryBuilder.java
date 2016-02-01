package org.infinispan.client.hotrod.impl.query;

import org.infinispan.client.hotrod.impl.RemoteCacheImpl;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.impl.BaseQueryBuilder;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
final class RemoteQueryBuilder extends BaseQueryBuilder {

   private static final Log log = LogFactory.getLog(RemoteQueryBuilder.class);
   private static final boolean trace = log.isTraceEnabled();

   private final RemoteCacheImpl cache;
   private final SerializationContext serializationContext;

   RemoteQueryBuilder(RemoteQueryFactory queryFactory, RemoteCacheImpl cache, SerializationContext serializationContext, String rootType) {
      super(queryFactory, rootType);
      this.cache = cache;
      this.serializationContext = serializationContext;
   }

   @Override
   public Query build() {
      RemoteJPAQueryGenerator generator = new RemoteJPAQueryGenerator(serializationContext);
      String jpqlString = accept(generator);
      if (trace) {
         log.tracef("JPQL string : %s", jpqlString);
      }
      return new RemoteQuery(queryFactory, cache, serializationContext, jpqlString, generator.getNamedParameters(), getProjectionPaths(), startOffset, maxResults);
   }
}
