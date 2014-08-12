package org.infinispan.query.dsl.impl;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public class DummyQueryFactory extends BaseQueryFactory<DummyQuery> {

   @Override
   public DummyQueryBuilder from(Class entityType) {
      return new DummyQueryBuilder(this, entityType.getCanonicalName());
   }

   @Override
   public DummyQueryBuilder from(String entityType) {
      return new DummyQueryBuilder(this, entityType);
   }
}
