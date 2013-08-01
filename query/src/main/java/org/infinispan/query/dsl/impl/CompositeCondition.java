package org.infinispan.query.dsl.impl;

import org.infinispan.query.dsl.QueryBuilder;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
class CompositeCondition extends BaseCondition {

   private final boolean isConjunction;

   private BaseCondition leftCondition;

   private BaseCondition rightCondition;

   public CompositeCondition(boolean isConjunction, BaseCondition leftCondition, BaseCondition rightCondition) {
      this.isConjunction = isConjunction;
      this.leftCondition = leftCondition;
      this.rightCondition = rightCondition;
   }

   @Override
   public <ReturnType> ReturnType accept(Visitor<ReturnType> visitor) {
      return visitor.visit(this);
   }

   public boolean isConjunction() {
      return isConjunction;
   }

   public BaseCondition getLeftCondition() {
      return leftCondition;
   }

   public BaseCondition getRightCondition() {
      return rightCondition;
   }

   public void replaceChild(BaseCondition oldChild, BaseCondition newChild) {
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

   @Override
   public String toString() {
      return "(" + leftCondition + ") " + (isConjunction ? "AND" : "OR") + " (" + rightCondition + ")";
   }
}
