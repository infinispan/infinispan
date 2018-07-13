package org.infinispan.query.dsl.impl;

import org.infinispan.query.dsl.QueryBuilder;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.impl.logging.Log;
import org.jboss.logging.Logger;

/**
 * Unary or binary boolean condition (NOT, AND, OR).
 *
 * @author anistor@redhat.com
 * @since 6.0
 */
abstract class BooleanCondition extends BaseCondition {

   private static final Log log = Logger.getMessageLogger(Log.class, BooleanCondition.class.getName());

   private BaseCondition leftCondition;

   private BaseCondition rightCondition;

   protected BooleanCondition(QueryFactory queryFactory, BaseCondition leftCondition, BaseCondition rightCondition) {
      super(queryFactory);
      if (leftCondition == rightCondition) {
         throw log.leftAndRightCannotBeTheSame();
      }
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
         throw log.conditionNotFoundInParent();
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
