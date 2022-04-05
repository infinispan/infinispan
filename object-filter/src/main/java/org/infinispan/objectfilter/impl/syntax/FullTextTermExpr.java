package org.infinispan.objectfilter.impl.syntax;

import java.util.Map;

/**
 * @author anistor@redhat.com
 * @since 9.0
 */
public final class FullTextTermExpr implements PrimaryPredicateExpr {

   private final ValueExpr leftChild;
   private final String term;
   private final Integer fuzzySlop;
   private final ConstantValueExpr.ParamPlaceholder paramPlaceholder;

   public FullTextTermExpr(ValueExpr leftChild, Object comparisonObject, Integer fuzzySlop) {
      this.leftChild = leftChild;
      this.term = comparisonObject.toString();
      this.fuzzySlop = fuzzySlop;
      this.paramPlaceholder = (comparisonObject instanceof ConstantValueExpr.ParamPlaceholder) ?
            (ConstantValueExpr.ParamPlaceholder) comparisonObject : null;
   }

   public Integer getFuzzySlop() {
      return fuzzySlop;
   }

   @Override
   public <T> T acceptVisitor(Visitor<?, ?> visitor) {
      return (T) visitor.visit(this);
   }

   @Override
   public ValueExpr getChild() {
      return leftChild;
   }

   @Override
   public String toString() {
      return leftChild.toString() + ":'" + term + "'" + (fuzzySlop != null ? "~" + fuzzySlop : "");
   }

   @Override
   public String toQueryString() {
      return leftChild.toQueryString() + ":'" + term + "'" + (fuzzySlop != null ? "~" + fuzzySlop : "");
   }

   public String getTerm(Map<String, Object> namedParameters) {
      if (paramPlaceholder == null) {
         return term;
      }

      String paramName = paramPlaceholder.getName();
      if (namedParameters == null) {
         throw new IllegalStateException("Missing value for parameter " + paramName);
      }

      Comparable value = (Comparable) namedParameters.get(paramName);
      if (value == null) {
         throw new IllegalStateException("Missing value for parameter " + paramName);
      }
      if (value instanceof String) {
         return (String) value;
      }

      throw new IllegalStateException("Parameter must be a string " + paramName);
   }
}
