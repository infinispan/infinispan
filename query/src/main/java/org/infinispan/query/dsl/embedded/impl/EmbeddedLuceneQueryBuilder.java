package org.infinispan.query.dsl.embedded.impl;

import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.embedded.LuceneQuery;
import org.infinispan.query.dsl.impl.BaseQueryBuilder;
import org.infinispan.query.dsl.impl.JPAQueryGenerator;
import org.infinispan.query.logging.Log;
import org.infinispan.util.logging.LogFactory;

//TODO [anistor] remove this class in infinispan-8.0.0.Final

/**
 * @author anistor@redhat.com
 * @since 6.0
 * @deprecated replaced by {@link EmbeddedQueryBuilder}
 */
final class EmbeddedLuceneQueryBuilder extends BaseQueryBuilder<LuceneQuery> {

   private static final Log log = LogFactory.getLog(EmbeddedLuceneQueryBuilder.class, Log.class);

   private final QueryEngine queryEngine;

   EmbeddedLuceneQueryBuilder(QueryFactory queryFactory, QueryEngine queryEngine, String rootType) {
      super(queryFactory, rootType);
      this.queryEngine = queryEngine;
   }

   @Override
   public LuceneQuery build() {
      JPAQueryGenerator generator = new JPAQueryGenerator();
      String jpqlString = accept(generator);
      if (log.isTraceEnabled()) {
         log.tracef("JPQL string : %s", jpqlString);
      }
      return new EmbeddedLuceneQuery(queryEngine, queryFactory, jpqlString, generator.getNamedParameters(), getProjectionPaths(), startOffset, maxResults);
   }
}
