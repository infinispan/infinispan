package org.infinispan.objectfilter.impl.syntax;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public interface Visitor<BE, VE> {

   BE visit(NotExpr notExpr);

   BE visit(OrExpr orExpr);

   BE visit(AndExpr andExpr);

   BE visit(ConstantBooleanExpr constantBooleanExpr);

   BE visit(IsNullExpr isNullExpr);

   BE visit(ComparisonExpr comparisonExpr);

   BE visit(LikeExpr likeExpr);

   VE visit(ConstantValueExpr constantValueExpr);

   VE visit(PropertyValueExpr propertyValueExpr);

   VE visit(AggregationExpr aggregationExpr);
}
