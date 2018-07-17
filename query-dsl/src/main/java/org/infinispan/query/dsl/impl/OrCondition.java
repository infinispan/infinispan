package org.infinispan.query.dsl.impl;

import org.infinispan.query.dsl.QueryFactory;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
final class OrCondition extends BooleanCondition {

   OrCondition(QueryFactory queryFactory, BaseCondition leftCondition, BaseCondition rightCondition) {
      super(queryFactory, leftCondition, rightCondition);
   }

   @Override
   public <ReturnType> ReturnType accept(Visitor<ReturnType> visitor) {
      return visitor.visit(this);
   }

   @Override
   public String toString() {
      return "(" + getFirstCondition() + ") OR (" + getSecondCondition() + ")";
   }
}
