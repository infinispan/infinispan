package org.infinispan.objectfilter.impl.syntax;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public interface Visitor {

   BooleanExpr visit(NotExpr notExpr);

   BooleanExpr visit(OrExpr orExpr);

   BooleanExpr visit(AndExpr andExpr);

   BooleanExpr visit(ConstantBooleanExpr constantBooleanExpr);

   BooleanExpr visit(IsNullExpr isNullExpr);

   BooleanExpr visit(ComparisonExpr comparisonExpr);

   BooleanExpr visit(RegexExpr regexExpr);

   ValueExpr visit(ConstantValueExpr constantValueExpr);

   ValueExpr visit(PropertyValueExpr propertyValueExpr);
}
