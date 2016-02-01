package org.infinispan.query.dsl.impl;

import org.infinispan.query.dsl.Expression;
import org.infinispan.query.dsl.FilterConditionBeginContext;
import org.infinispan.query.dsl.FilterConditionContext;
import org.infinispan.query.dsl.FilterConditionEndContext;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.impl.logging.Log;
import org.jboss.logging.Logger;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
public abstract class BaseQueryFactory implements QueryFactory {

   private static final Log log = Logger.getMessageLogger(Log.class, BaseQueryFactory.class.getName());

   @Override
   public FilterConditionEndContext having(String attributePath) {
      return having(Expression.property(attributePath));
   }

   @Override
   public FilterConditionEndContext having(Expression expression) {
      return new AttributeCondition(this, expression);
   }

   @Override
   public FilterConditionBeginContext not() {
      return new IncompleteCondition(this).not();
   }

   @Override
   public FilterConditionContext not(FilterConditionContext fcc) {
      if (fcc == null) {
         throw log.argumentCannotBeNull();
      }

      BaseCondition baseCondition = ((BaseCondition) fcc).getRoot();
      if (baseCondition.queryFactory != this) {
         throw log.conditionWasCreatedByAnotherFactory();
      }
      if (baseCondition.queryBuilder != null) {
         throw log.conditionIsAlreadyInUseByAnotherBuilder();
      }
      NotCondition notCondition = new NotCondition(this, baseCondition);
      baseCondition.setParent(notCondition);
      return notCondition;
   }
}
