package org.infinispan.query.dsl.impl;

import org.infinispan.query.dsl.Query;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
class DummyQueryFactory extends BaseQueryFactory {

   @Override
   public Query create(String queryString) {
      return new DummyQuery();
   }

   @Override
   public DummyQueryBuilder from(Class<?> entityType) {
      return new DummyQueryBuilder(this, entityType.getName());
   }

   @Override
   public DummyQueryBuilder from(String entityType) {
      return new DummyQueryBuilder(this, entityType);
   }
}
