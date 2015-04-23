package org.infinispan.query.dsl.embedded.impl;

import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.impl.BaseQueryBuilder;
import org.infinispan.query.dsl.impl.JPAQueryGenerator;
import org.infinispan.query.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
final class EmbeddedQueryBuilder extends BaseQueryBuilder<Query> {

   private static final Log log = LogFactory.getLog(EmbeddedQueryBuilder.class, Log.class);

   private final QueryEngine queryEngine;

   public EmbeddedQueryBuilder(EmbeddedQueryFactory queryFactory, QueryEngine queryEngine, String rootType) {
      super(queryFactory, rootType);
      this.queryEngine = queryEngine;
   }

   @Override
   public Query build() {
      String jpqlString = accept(new JPAQueryGenerator());
      if (log.isTraceEnabled()) {
         log.tracef("JPQL string : %s", jpqlString);
      }
      return queryEngine.buildQuery(queryFactory, jpqlString, startOffset, maxResults);
   }
}
