package org.infinispan.query.dsl.impl;

import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;

/**
 * @author anistor@redhat.com
 * @since 7.2
 */
public abstract class BaseQuery implements Query {

   protected final QueryFactory queryFactory;

   protected final String jpaQuery;

   public BaseQuery(QueryFactory queryFactory, String jpaQuery) {
      this.queryFactory = queryFactory;
      this.jpaQuery = jpaQuery;
   }

   public QueryFactory getQueryFactory() {
      return queryFactory;
   }

   public String getJPAQuery() {
      return jpaQuery;
   }
}
