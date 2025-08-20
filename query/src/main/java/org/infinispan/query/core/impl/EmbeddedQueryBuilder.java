package org.infinispan.query.core.impl;

import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.impl.BaseQueryBuilder;
import org.infinispan.query.dsl.impl.QueryStringCreator;
import org.infinispan.util.logging.LogFactory;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
final class EmbeddedQueryBuilder extends BaseQueryBuilder {

   private static final Log log = LogFactory.getLog(EmbeddedQueryBuilder.class, Log.class);

   private final QueryEngine<?> queryEngine;

   EmbeddedQueryBuilder(EmbeddedQueryFactory queryFactory, QueryEngine<?> queryEngine, String rootType) {
      super(queryFactory, rootType);
      this.queryEngine = queryEngine;
   }

   @Override
   public <T> Query<T> build() {
      QueryStringCreator generator = new QueryStringCreator();
      String queryString = accept(generator);
      if (log.isTraceEnabled()) {
         log.tracef("Query string : %s", queryString);
      }
      return new DelegatingQuery<>(queryEngine, queryFactory, queryString, generator.getNamedParameters(), getProjectionPaths(), startOffset, maxResults, false);
   }
}
