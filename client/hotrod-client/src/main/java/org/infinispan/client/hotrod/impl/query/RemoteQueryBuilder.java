package org.infinispan.client.hotrod.impl.query;

import org.infinispan.client.hotrod.impl.RemoteCacheImpl;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.impl.BaseQueryBuilder;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
public final class RemoteQueryBuilder extends BaseQueryBuilder<Query> {

   private static final Log log = LogFactory.getLog(RemoteQueryBuilder.class);

   private final RemoteCacheImpl cache;

   public RemoteQueryBuilder(RemoteCacheImpl cache, Class rootType) {
      super(rootType);
      this.cache = cache;
   }

   @Override
   public Query build() {
      String jpqlString = accept(new RemoteJPAQueryGenerator(ProtoStreamMarshaller.getSerializationContext()));
      log.tracef("JPQL string : %s", jpqlString);
      return new RemoteQuery(cache, jpqlString, sortCriteria, startOffset, maxResults);
   }
}
