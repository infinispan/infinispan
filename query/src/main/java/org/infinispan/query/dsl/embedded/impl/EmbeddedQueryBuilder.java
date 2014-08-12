package org.infinispan.query.dsl.embedded.impl;

import org.infinispan.AdvancedCache;
import org.infinispan.objectfilter.impl.ReflectionMatcher;
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

   private static final Log log = LogFactory.getLog(EmbeddedLuceneQueryBuilder.class, Log.class);

   private final AdvancedCache<?, ?> cache;

   public EmbeddedQueryBuilder(EmbeddedQueryFactory queryFactory, AdvancedCache<?, ?> cache, String rootType) {
      super(queryFactory, rootType);
      this.cache = cache;
   }

   @Override
   public Query build() {
      String jpqlString = accept(new JPAQueryGenerator());
      if (log.isTraceEnabled()) {
         log.tracef("JPQL string : %s", jpqlString);
      }

      return new EmbeddedQuery(cache, jpqlString, startOffset, maxResults, ReflectionMatcher.class);
   }
}
