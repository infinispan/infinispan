package org.infinispan.query.dsl.impl;

import org.infinispan.query.dsl.FilterConditionBeginContext;
import org.infinispan.query.dsl.FilterConditionContext;
import org.infinispan.query.dsl.FilterConditionEndContext;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
public abstract class BaseQueryFactory<T extends Query> implements QueryFactory<T> {

   @Override
   public FilterConditionEndContext having(String attributePath) {
      return new AttributeCondition(this, attributePath);
   }

   @Override
   public FilterConditionBeginContext not() {
      return new IncompleteCondition(this).not();
   }

   @Override
   public FilterConditionContext not(FilterConditionContext fcc) {
      if (fcc == null) {
         throw new IllegalArgumentException("Argument cannot be null");
      }

      BaseCondition baseCondition = ((BaseCondition) fcc).getRoot();
      if (baseCondition.queryFactory != this) {
         throw new IllegalArgumentException("The given condition was created by a different factory");
      }
      if (baseCondition.queryBuilder != null) {
         throw new IllegalArgumentException("The given condition is already in use by another builder");
      }
      NotCondition notCondition = new NotCondition(this, baseCondition);
      baseCondition.setParent(notCondition);
      return notCondition;
   }
}
