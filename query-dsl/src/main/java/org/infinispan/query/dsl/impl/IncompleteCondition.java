package org.infinispan.query.dsl.impl;

import org.infinispan.query.dsl.Expression;
import org.infinispan.query.dsl.FilterConditionBeginContext;
import org.infinispan.query.dsl.FilterConditionContext;
import org.infinispan.query.dsl.FilterConditionEndContext;
import org.infinispan.query.dsl.QueryBuilder;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.impl.logging.Log;
import org.jboss.logging.Logger;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
final class IncompleteCondition extends BaseCondition implements FilterConditionBeginContext {

   private static final Log log = Logger.getMessageLogger(Log.class, IncompleteCondition.class.getName());

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
         throw log.incompleteCondition();
      }

      return filterCondition.accept(visitor);
   }

   @Override
   public FilterConditionEndContext having(Expression expression) {
      if (filterCondition != null) {
         throw log.cannotUseOperatorAgain("having(..)");
      }
      AttributeCondition attributeCondition = new AttributeCondition(queryFactory, expression);
      attributeCondition.setNegated(isNegated);
      attributeCondition.setQueryBuilder(queryBuilder);
      attributeCondition.setParent(this);
      filterCondition = attributeCondition;
      return attributeCondition;
   }

   @Override
   public FilterConditionEndContext having(String attributePath) {
      return having(Expression.property(attributePath));
   }

   @Override
   public BaseCondition not() {
      if (filterCondition != null) {
         throw log.cannotUseOperatorAgain("not()");
      }

      isNegated = !isNegated;
      return this;
   }

   @Override
   public BaseCondition not(FilterConditionContext fcc) {
      if (fcc == null) {
         throw log.argumentCannotBeNull();
      }

      if (filterCondition != null) {
         throw log.cannotUseOperatorAgain("not(..)");
      }

      BaseCondition baseCondition = ((BaseCondition) fcc).getRoot();
      if (baseCondition.queryFactory != queryFactory) {
         throw log.conditionWasCreatedByAnotherFactory();
      }
      if (baseCondition.queryBuilder != null) {
         throw log.conditionIsAlreadyInUseByAnotherBuilder();
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
