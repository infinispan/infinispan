package org.infinispan.objectfilter.impl.syntax.parser;

import java.util.List;

import org.infinispan.objectfilter.impl.logging.Log;
import org.infinispan.objectfilter.impl.ql.PropertyPath;
import org.infinispan.objectfilter.impl.ql.QueryRendererDelegate;
import org.infinispan.objectfilter.impl.syntax.ComparisonExpr;
import org.infinispan.objectfilter.impl.syntax.ConstantValueExpr;
import org.jboss.logging.Logger;

public class VirtualExpressionBuilder<TypeMetadata> {

   private static final Log log = Logger.getMessageLogger(Log.class, VirtualExpressionBuilder.class.getName());

   private final QueryRendererDelegateImpl<TypeMetadata> owner;

   private final ExpressionBuilder<TypeMetadata> whereBuilder;

   private final ExpressionBuilder<TypeMetadata> havingBuilder;

   private final ExpressionBuilder<TypeMetadata> filteringBuilder;

   public VirtualExpressionBuilder(QueryRendererDelegateImpl<TypeMetadata> owner, ObjectPropertyHelper<TypeMetadata> propertyHelper) {
      this.owner = owner;
      this.whereBuilder = new ExpressionBuilder<>(propertyHelper);
      this.havingBuilder = new ExpressionBuilder<>(propertyHelper);
      this.filteringBuilder = new ExpressionBuilder<>(propertyHelper);
   }

   public void setEntityType(TypeMetadata targetEntityMetadata) {
      whereBuilder.setEntityType(targetEntityMetadata);
      havingBuilder.setEntityType(targetEntityMetadata);
      filteringBuilder.setEntityType(targetEntityMetadata);
   }

   public ExpressionBuilder<TypeMetadata> whereBuilder() {
      return whereBuilder;
   }

   public ExpressionBuilder<TypeMetadata> havingBuilder() {
      return havingBuilder;
   }

   public ExpressionBuilder<TypeMetadata> filteringBuilder() {
      return filteringBuilder;
   }

   public void pushOr() {
      builder().pushOr();
   }

   public void pushAnd() {
      builder().pushAnd();
   }

   public void pushNot() {
      builder().pushNot();
   }

   public void addComparison(PropertyPath<TypeDescriptor<TypeMetadata>> property, ComparisonExpr.Type comparisonType, Object comparisonValue) {
      builder().addComparison(property, comparisonType, comparisonValue);
   }

   public void addIn(PropertyPath<TypeDescriptor<TypeMetadata>> property, List<Object> values) {
      builder().addIn(property, values);
   }

   public void addRange(PropertyPath<TypeDescriptor<TypeMetadata>> property, Object lowerComparisonValue, Object upperComparisonValue) {
      builder().addRange(property, lowerComparisonValue, upperComparisonValue);
   }

   public void addLike(PropertyPath<TypeDescriptor<TypeMetadata>> property, Object pattern, Character escapeCharacter) {
      builder().addLike(property, pattern, escapeCharacter);
   }

   public void addIsNull(PropertyPath<TypeDescriptor<TypeMetadata>> property) {
      builder().addIsNull(property);
   }

   public void addConstantBoolean(boolean booleanConstant) {
      builder().addConstantBoolean(booleanConstant);
   }

   public void popBoolean() {
      builder().pop();
   }

   public void addFullTextTerm(PropertyPath<TypeDescriptor<TypeMetadata>> property, Object comparisonObject, Integer fuzzy) {
      fullTextBuilder().addFullTextTerm(property, comparisonObject, fuzzy);
   }

   public void addFullTextRegexp(PropertyPath<TypeDescriptor<TypeMetadata>> property, String term) {
      fullTextBuilder().addFullTextRegexp(property, term);
   }

   public void addFullTextRange(PropertyPath<TypeDescriptor<TypeMetadata>> property, boolean includeLower, Object from, Object to, boolean includeUpper) {
      fullTextBuilder().addFullTextRange(property, includeLower, from, to, includeUpper);
   }

   public void pushFullTextBoost(float boost) {
      fullTextBuilder().pushFullTextBoost(boost);
   }

   public void popFullTextBoost() {
      fullTextBuilder().pop();
   }

   public void pushFullTextOccur(QueryRendererDelegate.Occur occur) {
      fullTextBuilder().pushFullTextOccur(occur);
   }

   public void popFullTextOccur() {
      fullTextBuilder().pop();
   }

   public void addKnnPredicate(PropertyPath<TypeDescriptor<TypeMetadata>> property, Class<?> expectedType, List<Object> vector, Object knn) {
      knnBuilder().addKnnPredicate(property, expectedType, vector, knn);
   }

   public void addKnnPredicate(PropertyPath<TypeDescriptor<TypeMetadata>> property, Class<?> expectedType, ConstantValueExpr.ParamPlaceholder vectorParam, Object knn) {
      knnBuilder().addKnnPredicate(property, expectedType, vectorParam, knn);
   }

   private ExpressionBuilder<TypeMetadata> builder() {
      if (phase() == QueryRendererDelegateImpl.Phase.WHERE) {
         return (filtering()) ? filteringBuilder : whereBuilder;
      } else if (phase() == QueryRendererDelegateImpl.Phase.HAVING) {
         return havingBuilder;
      } else {
         throw new IllegalStateException();
      }
   }

   private ExpressionBuilder<TypeMetadata> fullTextBuilder() {
      if (phase() == QueryRendererDelegateImpl.Phase.WHERE) {
         return (filtering()) ? filteringBuilder : whereBuilder;
      } else if (phase() == QueryRendererDelegateImpl.Phase.HAVING) {
         throw log.getFullTextQueriesNotAllowedInHavingClauseException();
      } else {
         throw new IllegalStateException();
      }
   }

   private ExpressionBuilder<TypeMetadata> knnBuilder() {
      if (phase() == QueryRendererDelegateImpl.Phase.WHERE) {
         if (filtering()) {
            throw log.knnPredicateOnFilteringClause();
         }
         return whereBuilder;
      } else if (phase() == QueryRendererDelegateImpl.Phase.HAVING) {
         throw log.knnPredicateOnHavingClause();
      } else {
         throw new IllegalStateException();
      }
   }

   private QueryRendererDelegateImpl.Phase phase() {
      return owner.phase;
   }

   private boolean filtering() {
      return owner.filtering;
   }
}
