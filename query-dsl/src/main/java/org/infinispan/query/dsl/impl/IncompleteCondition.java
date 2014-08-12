package org.infinispan.query.dsl.impl;

import org.infinispan.query.dsl.FilterConditionBeginContext;
import org.infinispan.query.dsl.FilterConditionContext;
import org.infinispan.query.dsl.FilterConditionEndContext;
import org.infinispan.query.dsl.QueryBuilder;
import org.infinispan.query.dsl.QueryFactory;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
class IncompleteCondition extends BaseCondition implements FilterConditionBeginContext {

   private boolean isNegated = false;

   private BaseCondition filterCondition;

   IncompleteCondition(QueryFactory queryFactory) {
      super(queryFactory);
   }

   @Override
   void setQueryBuilder(QueryBuilder queryBuilder) {
      super.setQueryBuilder(queryBuilder);
      if (filterCondition != null) {
         filterCondition.setQueryBuilder(queryBuilder);
      }
   }

   @Override
   public <ReturnType> ReturnType accept(Visitor<ReturnType> visitor) {
      if (filterCondition == null) {
         throw new IllegalStateException("Cannot visit an incomplete condition.");
      }

      return filterCondition.accept(visitor);
   }

   @Override
   public FilterConditionEndContext having(String attributePath) {
      if (filterCondition != null) {
         throw new IllegalStateException("Sentence already started. Cannot use 'having(..)' again.");
      }
      AttributeCondition attributeCondition = new AttributeCondition(queryFactory, attributePath);
      attributeCondition.setNegated(isNegated);
      attributeCondition.setQueryBuilder(queryBuilder);
      attributeCondition.setParent(this);
      filterCondition = attributeCondition;
      return attributeCondition;
   }

   @Override
   public FilterConditionBeginContext not() {
      if (filterCondition != null) {
         throw new IllegalStateException("Sentence already started. Cannot use 'not()' again.");
      }

      isNegated = !isNegated;
      return this;
   }

   @Override
   public FilterConditionContext not(FilterConditionContext fcc) {
      if (fcc == null) {
         throw new IllegalArgumentException("Argument cannot be null");
      }

      if (filterCondition != null) {
         throw new IllegalStateException("Sentence already started. Cannot use 'not(..)' again.");
      }

      BaseCondition baseCondition = ((BaseCondition) fcc).getRoot();
      if (baseCondition.queryFactory != queryFactory) {
         throw new IllegalArgumentException("The given condition was created by a different factory");
      }
      if (baseCondition.queryBuilder != null) {
         throw new IllegalArgumentException("The given condition is already in use by another builder");
      }

      isNegated = !isNegated;
      if (isNegated) {
         NotCondition notCondition = new NotCondition(queryFactory, baseCondition);
         notCondition.setQueryBuilder(queryBuilder);
         filterCondition = notCondition;
      } else {
         baseCondition.setQueryBuilder(queryBuilder);
         filterCondition = baseCondition;
      }
      return filterCondition;
   }
}
