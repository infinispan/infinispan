package org.infinispan.objectfilter.impl.syntax;

/**
 * A pass-through, zero-transformation {@link Visitor} implementation. Comes handy when you want to implement a {@link
 * Visitor} but do not want to cover all the cases.
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
public class ExprVisitor implements Visitor<BooleanExpr, ValueExpr> {

   @Override
   public BooleanExpr visit(FullTextOccurExpr fullTextOccurExpr) {
      return fullTextOccurExpr;
   }

   @Override
   public BooleanExpr visit(FullTextBoostExpr fullTextBoostExpr) {
      return fullTextBoostExpr;
   }

   @Override
   public BooleanExpr visit(FullTextTermExpr fullTextTermExpr) {
      return fullTextTermExpr;
   }

   @Override
   public BooleanExpr visit(FullTextRegexpExpr fullTextRegexpExpr) {
      return fullTextRegexpExpr;
   }

   @Override
   public BooleanExpr visit(FullTextRangeExpr fullTextRangeExpr) {
      return fullTextRangeExpr;
   }

   @Override
   public BooleanExpr visit(NotExpr notExpr) {
      return notExpr;
   }

   @Override
   public BooleanExpr visit(OrExpr orExpr) {
      return orExpr;
   }

   @Override
   public BooleanExpr visit(AndExpr andExpr) {
      return andExpr;
   }

   @Override
   public BooleanExpr visit(ConstantBooleanExpr constantBooleanExpr) {
      return constantBooleanExpr;
   }

   @Override
   public BooleanExpr visit(IsNullExpr isNullExpr) {
      return isNullExpr;
   }

   @Override
   public BooleanExpr visit(ComparisonExpr comparisonExpr) {
      return comparisonExpr;
   }

   @Override
   public BooleanExpr visit(BetweenExpr betweenExpr) {
      return betweenExpr;
   }

   @Override
   public BooleanExpr visit(LikeExpr likeExpr) {
      return likeExpr;
   }

   @Override
   public BooleanExpr visit(GeofiltExpr geofiltExpr) {
      return geofiltExpr;
   }

   @Override
   public ValueExpr visit(ConstantValueExpr constantValueExpr) {
      return constantValueExpr;
   }

   @Override
   public ValueExpr visit(PropertyValueExpr propertyValueExpr) {
      return propertyValueExpr;
   }

   @Override
   public ValueExpr visit(AggregationExpr aggregationExpr) {
      return aggregationExpr;
   }
}
