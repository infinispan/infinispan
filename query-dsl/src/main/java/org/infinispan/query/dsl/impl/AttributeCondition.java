package org.infinispan.query.dsl.impl;

import org.infinispan.query.dsl.FilterConditionContext;
import org.infinispan.query.dsl.FilterConditionEndContext;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.RangeConditionContext;

import java.util.Collection;

//todo [anistor] i18n for exception messages

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
class AttributeCondition extends BaseCondition implements FilterConditionEndContext, RangeConditionContext {

   private final String attributePath;

   private boolean isNegated;

   private OperatorAndArgument operatorAndArgument;

   public AttributeCondition(QueryFactory queryFactory, String attributePath) {
      super(queryFactory);
      this.attributePath = attributePath;
   }

   OperatorAndArgument getOperatorAndArgument() {
      return operatorAndArgument;
   }

   String getAttributePath() {
      return attributePath;
   }

   boolean isNegated() {
      return isNegated;
   }

   void setNegated(boolean negated) {
      isNegated = negated;
   }

   @Override
   public FilterConditionContext in(Object... values) {
      if (values == null || values.length == 0) {
         throw new IllegalArgumentException("The list of values for 'in(..)' cannot be null or empty");
      }
      setOperatorAndArgument(new InOperator(this, values));
      return this;
   }

   @Override
   public FilterConditionContext in(Collection values) {
      if (values == null || values.isEmpty()) {
         throw new IllegalArgumentException("The list of values for 'in(..)' cannot be null or empty");
      }
      setOperatorAndArgument(new InOperator(this, values));
      return this;
   }

   @Override
   public FilterConditionContext like(String pattern) {
      setOperatorAndArgument(new LikeOperator(this, pattern));
      return this;
   }

   @Override
   public FilterConditionContext contains(Object value) {
      setOperatorAndArgument(new ContainsOperator(this, value));
      return this;
   }

   @Override
   public FilterConditionContext containsAll(Object... values) {
      setOperatorAndArgument(new ContainsAllOperator(this, values));
      return this;
   }

   @Override
   public FilterConditionContext containsAll(Collection values) {
      setOperatorAndArgument(new ContainsAllOperator(this, values));
      return this;
   }

   @Override
   public FilterConditionContext containsAny(Object... values) {
      setOperatorAndArgument(new ContainsAnyOperator(this, values));
      return this;
   }

   @Override
   public FilterConditionContext containsAny(Collection values) {
      setOperatorAndArgument(new ContainsAnyOperator(this, values));
      return this;
   }

   @Override
   public FilterConditionContext isNull() {
      setOperatorAndArgument(new IsNullOperator(this));
      return this;
   }

   @Override
   public FilterConditionContext eq(Object value) {
      setOperatorAndArgument(new EqOperator(this, value));
      return this;
   }

   @Override
   public FilterConditionContext lt(Object value) {
      setOperatorAndArgument(new LtOperator(this, value));
      return this;
   }

   @Override
   public FilterConditionContext lte(Object value) {
      setOperatorAndArgument(new LteOperator(this, value));
      return this;
   }

   @Override
   public FilterConditionContext gt(Object value) {
      setOperatorAndArgument(new GtOperator(this, value));
      return this;
   }

   @Override
   public FilterConditionContext gte(Object value) {
      setOperatorAndArgument(new GteOperator(this, value));
      return this;
   }

   @Override
   public RangeConditionContext between(Object from, Object to) {
      setOperatorAndArgument(new BetweenOperator(this, new ValueRange(from, to)));
      return this;
   }

   @Override
   public RangeConditionContext includeLower(boolean includeLower) {
      ValueRange valueRange = (ValueRange) operatorAndArgument.getArgument();
      valueRange.setIncludeLower(includeLower);
      return this;
   }

   @Override
   public RangeConditionContext includeUpper(boolean includeUpper) {
      ValueRange valueRange = (ValueRange) operatorAndArgument.getArgument();
      valueRange.setIncludeUpper(includeUpper);
      return this;
   }

   private void setOperatorAndArgument(OperatorAndArgument operatorAndArgument) {
      operatorAndArgument.validate();

      if (this.operatorAndArgument != null) {
         throw new IllegalStateException("operator was already specified");
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
            ", attributePath='" + attributePath + '\'' +
            ", operatorAndArgument=" + operatorAndArgument +
            '}';
   }
}
