package org.infinispan.objectfilter.query;

import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryBuilder;
import org.infinispan.query.dsl.impl.BaseQueryFactory;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class FilterQueryFactory extends BaseQueryFactory<Query> {

   public FilterQueryFactory() {
   }

   @Override
   public QueryBuilder<Query> from(Class entityType) {
      return new FilterQueryBuilder(this, entityType.getCanonicalName());
   }

   @Override
   public QueryBuilder<Query> from(String entityType) {
      return new FilterQueryBuilder(this, entityType);
   }
}

