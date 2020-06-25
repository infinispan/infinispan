package org.infinispan.query.dsl.impl;

import org.infinispan.query.dsl.Query;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
class DummyQueryBuilder extends BaseQueryBuilder {

   DummyQueryBuilder(DummyQueryFactory queryFactory, String rootTypeName) {
      super(queryFactory, rootTypeName);
   }

   @Override
   public <T> Query<T> build() {
      return new DummyQuery<>();
   }
}
