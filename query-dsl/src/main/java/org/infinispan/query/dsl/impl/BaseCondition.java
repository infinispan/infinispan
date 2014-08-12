package org.infinispan.query.dsl.impl;

import org.infinispan.query.dsl.FilterConditionBeginContext;
import org.infinispan.query.dsl.FilterConditionContext;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryBuilder;
import org.infinispan.query.dsl.QueryFactory;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
abstract class BaseCondition implements FilterConditionContext, Visitable {

   protected BaseCondition parent = null;

   protected QueryBuilder queryBuilder;

   protected final QueryFactory queryFactory;

   protected BaseCondition(QueryFactory queryFactory) {
      this.queryFactory = queryFactory;
   }

   @Override
   public <T extends Query> QueryBuilder<T> toBuilder() {
      if (queryBuilder == null) {
         throw new IllegalStateException("This sub-query does not belong to a parent query builder yet");
      }
      return (QueryBuilder<T>)queryBuilder;
   }

   void setQueryBuilder(QueryBuilder queryBuilder) {
      if (this.queryBuilder != null) {
         throw new IllegalStateException("This query already belongs to a query builder");
      }
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

   public BaseCondition getParent() {
      return parent;
   }

   public void setParent(BaseCondition parent) {
      this.parent = parent;
   }

   @Override
   public FilterConditionBeginContext and() {
      IncompleteCondition rightCondition = new IncompleteCondition(queryFactory);
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
      IncompleteCondition rightCondition = new IncompleteCondition(queryFactory);
      combine(false, rightCondition);
      return rightCondition;
   }

   @Override
   public FilterConditionContext or(FilterConditionContext rightCondition) {
      combine(false, rightCondition);
      return this;
   }

   private void combine(boolean isConjunction, FilterConditionContext fcc) {
      if (fcc == null) {
         throw new IllegalArgumentException("Argument cannot be null");
      }

      BaseCondition rightCondition = ((BaseCondition) fcc).getRoot();
      if (rightCondition.queryFactory != queryFactory) {
         throw new IllegalArgumentException("The given condition was created by a different factory");
      }

      if (rightCondition.queryBuilder != null) {
         throw new IllegalArgumentException("The given condition is already in use by another builder");
      }

      if (isConjunction && parent instanceof OrCondition) {
         BooleanCondition p = new AndCondition(queryFactory, this, rightCondition);
         ((BooleanCondition) parent).replaceChildCondition(this, p);
         parent = p;
         rightCondition.setParent(p);
      } else {
         BaseCondition root = getRoot();
         BooleanCondition p = isConjunction ? new AndCondition(queryFactory, root, rightCondition) : new OrCondition(queryFactory, root, rightCondition);
         root.setParent(p);
         rightCondition.setParent(p);
      }

      rightCondition.setQueryBuilder(queryBuilder);
   }
}
