package org.infinispan.objectfilter.impl.syntax;

/**
 * Visitor interface for expressions.
 *
 * @param <BE> is the return type when visiting a boolean expression (an expression that produces a {@link Boolean})
 * @param <VE> is the return type when visiting a value expression (an expression that produces an arbitrary {@link
 *             Object})
 * @author anistor@redhat.com
 * @since 7.0
 */
public interface Visitor<BE, VE> {

   BE visit(FullTextOccurExpr fullTextOccurExpr);

   BE visit(FullTextBoostExpr fullTextBoostExpr);

   BE visit(FullTextTermExpr fullTextTermExpr);

   BE visit(FullTextRegexpExpr fullTextRegexpExpr);

   BE visit(FullTextRangeExpr fullTextRangeExpr);

   BE visit(NotExpr notExpr);

   BE visit(OrExpr orExpr);

   BE visit(AndExpr andExpr);

   BE visit(ConstantBooleanExpr constantBooleanExpr);

   BE visit(IsNullExpr isNullExpr);

   BE visit(ComparisonExpr comparisonExpr);

   BE visit(BetweenExpr betweenExpr);

   BE visit(LikeExpr likeExpr);

   BE visit(GeofiltExpr geofiltExpr);

   VE visit(ConstantValueExpr constantValueExpr);

   VE visit(PropertyValueExpr propertyValueExpr);

   VE visit(AggregationExpr aggregationExpr);
}
