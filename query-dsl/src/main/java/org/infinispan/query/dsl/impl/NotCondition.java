package org.infinispan.query.dsl.impl;

import org.infinispan.query.dsl.QueryFactory;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
class NotCondition extends BooleanCondition {

   public NotCondition(QueryFactory queryFactory, BaseCondition condition) {
      super(queryFactory, condition, null);
   }

   @Override
   public <ReturnType> ReturnType accept(Visitor<ReturnType> visitor) {
      return visitor.visit(this);
   }

   @Override
   public String toString() {
      return "NOT (" + getFirstCondition();
   }
}
