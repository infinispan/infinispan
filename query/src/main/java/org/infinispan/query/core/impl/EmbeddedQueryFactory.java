package org.infinispan.query.core.impl;

import org.infinispan.commons.query.BaseQuery;
import org.infinispan.query.dsl.QueryFactory;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class EmbeddedQueryFactory implements QueryFactory {

   private final QueryEngine<?> queryEngine;

   public EmbeddedQueryFactory(QueryEngine<?> queryEngine) {
      if (queryEngine == null) {
         throw new IllegalArgumentException("queryEngine cannot be null");
      }
      this.queryEngine = queryEngine;
   }

   @Override
   public <T> BaseQuery<T> create(String queryString) {
      return new DelegatingQuery<>(queryEngine, queryString);
   }
}
