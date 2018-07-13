package org.infinispan.query.dsl.impl;

import java.util.Collection;

import org.infinispan.query.dsl.Expression;
import org.infinispan.query.dsl.FilterConditionEndContext;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.RangeConditionContextQueryBuilder;
import org.infinispan.query.dsl.impl.logging.Log;
import org.jboss.logging.Logger;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
final class AttributeCondition extends BaseCondition implements FilterConditionEndContext, RangeConditionContextQueryBuilder {

   private static final Log log = Logger.getMessageLogger(Log.class, AttributeCondition.class.getName());

   private final Expression expression;

   private boolean isNegated;

   private OperatorAndArgument operatorAndArgument;

   public AttributeCondition(QueryFactory queryFactory, Expression expression) {
      super(queryFactory);
      this.expression = expression;
   }

   OperatorAndArgument getOperatorAndArgument() {
      return operatorAndArgument;
   }

   Expression getExpression() {
      return expression;
   }

   boolean isNegated() {
      return isNegated;
   }

   void setNegated(boolean negated) {
      isNegated = negated;
   }

   @Override
   public BaseCondition in(Object... values) {
      if (values == null || values.length == 0) {
         throw log.listOfValuesForInCannotBeNulOrEmpty();
      }
      setOperatorAndArgument(new InOperator(this, values));
      return this;
   }

   @Override
   public BaseCondition in(Collection values) {
      if (values == null || values.isEmpty()) {
         throw log.listOfValuesForInCannotBeNulOrEmpty();
      }
      setOperatorAndArgument(new InOperator(this, values));
      return this;
   }

   @Override
   public BaseCondition like(String pattern) {
      setOperatorAndArgument(new LikeOperator(this, pattern));
      return this;
   }

   @Override
   public BaseCondition contains(Object value) {
      setOperatorAndArgument(new ContainsOperator(this, value));
      return this;
   }

   @Override
   public BaseCondition containsAll(Object... values) {
      setOperatorAndArgument(new ContainsAllOperator(this, values));
      return this;
   }

   @Override
   public BaseCondition containsAll(Collection values) {
      setOperatorAndArgument(new ContainsAllOperator(this, values));
      return this;
   }

   @Override
   public BaseCondition containsAny(Object... values) {
      setOperatorAndArgument(new ContainsAnyOperator(this, values));
      return this;
   }

   @Override
   public BaseCondition containsAny(Collection values) {
      setOperatorAndArgument(new ContainsAnyOperator(this, values));
      return this;
   }

   @Override
   public BaseCondition isNull() {
      setOperatorAndArgument(new IsNullOperator(this));
      return this;
   }

   @Override
   public BaseCondition eq(Object value) {
      setOperatorAndArgument(new EqOperator(this, value));
      return this;
   }

   @Override
   public BaseCondition equal(Object value) {
      return eq(value);
   }

   @Override
   public BaseCondition lt(Object value) {
      setOperatorAndArgument(new LtOperator(this, value));
      return this;
   }

   @Override
   public BaseCondition lte(Object value) {
      setOperatorAndArgument(new LteOperator(this, value));
      return this;
   }

   @Override
   public BaseCondition gt(Object value) {
      setOperatorAndArgument(new GtOperator(this, value));
      return this;
   }

   @Override
   public BaseCondition gte(Object value) {
      setOperatorAndArgument(new GteOperator(this, value));
      return this;
   }

   @Override
   public AttributeCondition between(Object from, Object to) {
      setOperatorAndArgument(new BetweenOperator(this, new ValueRange(from, to)));
      return this;
   }

   @Override
   public AttributeCondition includeLower(boolean includeLower) {
      ValueRange valueRange = (ValueRange) operatorAndArgument.getArgument();
      valueRange.setIncludeLower(includeLower);
      return this;
   }

   @Override
   public AttributeCondition includeUpper(boolean includeUpper) {
      ValueRange valueRange = (ValueRange) operatorAndArgument.getArgument();
      valueRange.setIncludeUpper(includeUpper);
      return this;
   }

   private void setOperatorAndArgument(OperatorAndArgument operatorAndArgument) {
      operatorAndArgument.validate();

      if (this.operatorAndArgument != null) {
         throw log.operatorWasAlreadySpecified();
      }

      this.operatorAndArgument = operatorAndArgument;
   }

   @Override
   public <ReturnType> ReturnType accept(Visitor<ReturnType> visitor) {
      return visitor.visit(this);
   }

   @Override
   public String toString() {
      return "AttributeCondition{" +
            "isNegated=" + isNegated +
            ", expression='" + expression + '\'' +
            ", operatorAndArgument=" + operatorAndArgument +
            '}';
   }
}
