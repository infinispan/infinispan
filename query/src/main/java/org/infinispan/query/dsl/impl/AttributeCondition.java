package org.infinispan.query.dsl.impl;

import org.infinispan.query.dsl.FilterConditionBeginContext;
import org.infinispan.query.dsl.FilterConditionContext;
import org.infinispan.query.dsl.FilterConditionEndContext;
import org.infinispan.query.dsl.RangeConditionContext;

import java.util.Collection;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
class AttributeCondition extends BaseCondition
      implements FilterConditionBeginContext, FilterConditionEndContext, RangeConditionContext {

   @Override
   public <ReturnType> ReturnType accept(Visitor<ReturnType> visitor) {
      return visitor.visit(this);
   }

   protected enum Operator {
      IN,
      LIKE,
      CONTAINS,
      CONTAINS_ALL,
      CONTAINS_ANY,
      IS_NULL,
      EQ,
      LT,
      LTE,
      GT,
      GTE,
      BETWEEN
   }

   private Operator operator;

   private Object argument;

   private String attributePath;

   private boolean isNegated;

   public AttributeCondition() {
   }

   Operator getOperator() {
      return operator;
   }

   Object getArgument() {
      return argument;
   }

   String getAttributePath() {
      return attributePath;
   }

   boolean isNegated() {
      return isNegated;
   }

   @Override
   public FilterConditionEndContext having(String attributePath) {
      if (attributePath == null) {
         throw new IllegalArgumentException("attribute path cannot be null");
      }
      if (this.attributePath != null) {
         throw new IllegalStateException("attribute path was already specified");
      }
      this.attributePath = attributePath;
      return this;
   }

   @Override
   public FilterConditionBeginContext not() {
      isNegated = !isNegated;
      return this;
   }

   @Override
   public FilterConditionContext in(Object... value) {
      setOperatorAndArgument(Operator.IN, value);
      return this;
   }

   @Override
   public FilterConditionContext in(Collection values) {
      setOperatorAndArgument(Operator.IN, values);
      return this;
   }

   @Override
   public FilterConditionContext like(String pattern) {
      setOperatorAndArgument(Operator.LIKE, pattern);
      return this;
   }

   @Override
   public FilterConditionContext contains(Object value) {
      setOperatorAndArgument(Operator.CONTAINS, value);
      return this;
   }

   @Override
   public FilterConditionContext containsAll(Object... value) {
      setOperatorAndArgument(Operator.CONTAINS_ALL, value);
      return this;
   }

   @Override
   public FilterConditionContext containsAll(Collection values) {
      setOperatorAndArgument(Operator.CONTAINS_ALL, values);
      return this;
   }

   @Override
   public FilterConditionContext containsAny(Object... value) {
      setOperatorAndArgument(Operator.CONTAINS_ANY, value);
      return this;
   }

   @Override
   public FilterConditionContext containsAny(Collection values) {
      setOperatorAndArgument(Operator.CONTAINS_ANY, values);
      return this;
   }

   @Override
   public FilterConditionContext isNull() {
      setOperatorAndArgument(Operator.IS_NULL, null);
      return this;
   }

   @Override
   public FilterConditionContext eq(Object value) {
      setOperatorAndArgument(Operator.EQ, value);
      return this;
   }

   @Override
   public FilterConditionContext lt(Object value) {
      setOperatorAndArgument(Operator.LT, value);
      return this;
   }

   @Override
   public FilterConditionContext lte(Object value) {
      setOperatorAndArgument(Operator.LTE, value);
      return this;
   }

   @Override
   public FilterConditionContext gt(Object value) {
      setOperatorAndArgument(Operator.GT, value);
      return this;
   }

   @Override
   public FilterConditionContext gte(Object value) {
      setOperatorAndArgument(Operator.GTE, value);
      return this;
   }

   @Override
   public RangeConditionContext between(Object from, Object to) {
      ValueRange valueRange = new ValueRange(from, to);
      setOperatorAndArgument(Operator.BETWEEN, valueRange);
      return this;
   }

   @Override
   public RangeConditionContext includeLower(boolean includeLower) {
      ValueRange valueRange = (ValueRange) argument;
      valueRange.setIncludeLower(includeLower);
      return this;
   }

   @Override
   public RangeConditionContext includeUpper(boolean includeUpper) {
      ValueRange valueRange = (ValueRange) argument;
      valueRange.setIncludeUpper(includeUpper);
      return this;
   }

   private void setOperatorAndArgument(Operator operator, Object argument) {
      if (argument == null) {
         throw new IllegalArgumentException("argument cannot be null");
      }
      if (this.operator != null) {
         throw new IllegalStateException("operator was already specified");
      }
      this.operator = operator;
      this.argument = argument;
   }

   @Override
   public String toString() {
      return "AttributeCondition{" +
            "isNegated=" + isNegated +
            ", attributePath='" + attributePath + '\'' +
            ", operator=" + operator +
            ", argument=" + argument +
            '}';
   }
}
