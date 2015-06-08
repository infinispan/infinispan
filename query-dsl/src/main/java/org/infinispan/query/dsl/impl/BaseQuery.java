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

   protected final String[] projection;

   protected BaseQuery(QueryFactory queryFactory, String jpaQuery, String[] projection) {
      this.queryFactory = queryFactory;
      this.jpaQuery = jpaQuery;
      this.projection = projection != null && projection.length > 0 ? projection : null;
   }

   public QueryFactory getQueryFactory() {
      return queryFactory;
   }

   public String getJPAQuery() {
      return jpaQuery;
   }

   public String[] getProjection() {
      return projection;
   }
}
