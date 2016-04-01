package org.infinispan.query.dsl.impl;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
class DummyQueryBuilder extends BaseQueryBuilder {

   DummyQueryBuilder(DummyQueryFactory queryFactory, String rootTypeName) {
      super(queryFactory, rootTypeName);
   }

   @Override
   public DummyQuery build() {
      return new DummyQuery();
   }
}
