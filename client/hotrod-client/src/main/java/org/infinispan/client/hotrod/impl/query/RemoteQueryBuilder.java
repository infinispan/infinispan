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
public final class RemoteQueryBuilder extends BaseQueryBuilder<Query> {

   private static final Log log = LogFactory.getLog(RemoteQueryBuilder.class);

   private final RemoteCacheImpl cache;
   private final SerializationContext serializationContext;

   public RemoteQueryBuilder(RemoteQueryFactory queryFactory, RemoteCacheImpl cache, SerializationContext serializationContext, String rootType) {
      super(queryFactory, rootType);
      this.cache = cache;
      this.serializationContext = serializationContext;
   }

   @Override
   public Query build() {
      String jpqlString = accept(new RemoteJPAQueryGenerator(serializationContext));
      if (log.isTraceEnabled()) {
         log.tracef("JPQL string : %s", jpqlString);
      }
      String[] _projection = null;
      if (projection != null) {
         _projection = new String[projection.length];
         for (int i = 0; i < projection.length; i++) {
            _projection[i] = projection[i].toString();
         }
      }
      return new RemoteQuery(queryFactory, cache, serializationContext, jpqlString, _projection, startOffset, maxResults);
   }
}
