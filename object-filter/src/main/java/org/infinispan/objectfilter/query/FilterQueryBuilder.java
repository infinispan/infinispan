package org.infinispan.objectfilter.query;

import org.infinispan.objectfilter.impl.logging.Log;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.impl.BaseQueryBuilder;
import org.jboss.logging.Logger;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class FilterQueryBuilder extends BaseQueryBuilder<Query> {

   private static final Log log = Logger.getMessageLogger(Log.class, FilterQueryBuilder.class.getName());

   public FilterQueryBuilder(FilterQueryFactory queryFactory, String rootType) {
      super(queryFactory, rootType);
   }

   @Override
   public Query build() {
      String jpqlString = accept(new FilterJPAQueryGenerator());
      log.tracef("JPQL string : %s", jpqlString);
      return new FilterQuery(jpqlString, startOffset, maxResults);
   }
}
