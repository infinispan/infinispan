package org.infinispan.objectfilter.impl.syntax;

/**
 * A pass-through, zero transformation Visitor implementation. Comes handy when you want to implement a Visitor but do
 * not want to cover all the cases.
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
public class NoOpVisitor implements Visitor {

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
   public BooleanExpr visit(RegexExpr regexExpr) {
      return regexExpr;
   }

   @Override
   public ValueExpr visit(ConstantValueExpr constantValueExpr) {
      return constantValueExpr;
   }

   @Override
   public ValueExpr visit(PropertyValueExpr propertyValueExpr) {
      return propertyValueExpr;
   }
}
