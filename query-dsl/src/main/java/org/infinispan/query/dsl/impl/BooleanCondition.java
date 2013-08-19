package org.infinispan.query.dsl.impl;

import org.infinispan.query.dsl.QueryBuilder;

/**
 * Unary or binary boolean condition (NOT, AND, OR).
 *
 * @author anistor@redhat.com
 * @since 6.0
 */
abstract class BooleanCondition extends BaseCondition {

   private BaseCondition leftCondition;

   private BaseCondition rightCondition;

   public BooleanCondition(BaseCondition leftCondition, BaseCondition rightCondition) {
      this.leftCondition = leftCondition;
      this.rightCondition = rightCondition;
   }

   public BaseCondition getFirstCondition() {
      return leftCondition;
   }

   public BaseCondition getSecondCondition() {
      return rightCondition;
   }

   public void replaceChildCondition(BaseCondition oldChild, BaseCondition newChild) {
      if (leftCondition == oldChild) {
         leftCondition = newChild;
      } else if (rightCondition == oldChild) {
         rightCondition = newChild;
      } else {
         throw new IllegalStateException("Old child condition not found in parent");
      }
      newChild.setParent(this);
   }

   @Override
   void setQueryBuilder(QueryBuilder queryBuilder) {
      super.setQueryBuilder(queryBuilder);
      if (leftCondition != null) {
         leftCondition.setQueryBuilder(queryBuilder);
      }
      if (rightCondition != null) {
         rightCondition.setQueryBuilder(queryBuilder);
      }
   }
}
