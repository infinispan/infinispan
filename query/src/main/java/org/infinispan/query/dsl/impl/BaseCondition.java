package org.infinispan.query.dsl.impl;

import org.infinispan.query.dsl.FilterConditionBeginContext;
import org.infinispan.query.dsl.FilterConditionContext;
import org.infinispan.query.dsl.QueryBuilder;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
abstract class BaseCondition implements FilterConditionContext, Visitable {

   protected CompositeCondition parent = null;

   protected QueryBuilder queryBuilder;

   protected BaseCondition() {
   }

   @Override
   public QueryBuilder toBuilder() {
      if (queryBuilder == null) {
         throw new IllegalArgumentException("This sub-query does not belong to a parent query builder yet");
      }
      return queryBuilder;
   }

   void setQueryBuilder(QueryBuilder queryBuilder) {
      this.queryBuilder = queryBuilder;
   }

   /**
    * Returns the topmost condition, never {@code null}.
    *
    * @return the topmost condition following up the parent chain or {@code this} if parent is {@code null}.
    */
   public BaseCondition getRoot() {
      BaseCondition p = this;
      while (p.getParent() != null) {
         p = p.getParent();
      }
      return p;
   }

   public CompositeCondition getParent() {
      return parent;
   }

   public void setParent(CompositeCondition parent) {
      this.parent = parent;
   }

   @Override
   public FilterConditionBeginContext and() {
      AttributeCondition rightCondition = new AttributeCondition();
      combine(true, rightCondition);
      return rightCondition;
   }

   @Override
   public FilterConditionContext and(FilterConditionContext rightCondition) {
      combine(true, rightCondition);
      return this;
   }

   @Override
   public FilterConditionBeginContext or() {
      AttributeCondition rightCondition = new AttributeCondition();
      combine(false, rightCondition);
      return rightCondition;
   }

   @Override
   public FilterConditionContext or(FilterConditionContext rightCondition) {
      combine(false, rightCondition);
      return this;
   }

   private void combine(boolean isConjunction, FilterConditionContext fcc) {
      BaseCondition rightCondition = ((BaseCondition) fcc).getRoot();

      if (isConjunction && parent != null && !parent.isConjunction()) {
         CompositeCondition p = new CompositeCondition(true, this, rightCondition);
         parent.replaceChild(this, p);
         parent = p;
         rightCondition.setParent(p);
      } else {
         BaseCondition root = getRoot();
         CompositeCondition p = new CompositeCondition(isConjunction, root, rightCondition);
         root.setParent(p);
         rightCondition.setParent(p);
      }

      rightCondition.setQueryBuilder(queryBuilder);
   }
}
