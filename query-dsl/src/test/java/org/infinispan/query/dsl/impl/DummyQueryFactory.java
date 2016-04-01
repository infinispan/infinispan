package org.infinispan.query.dsl.impl;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
class DummyQueryFactory extends BaseQueryFactory {

   @Override
   public DummyQueryBuilder from(Class entityType) {
      return new DummyQueryBuilder(this, entityType.getName());
   }

   @Override
   public DummyQueryBuilder from(String entityType) {
      return new DummyQueryBuilder(this, entityType);
   }
}
