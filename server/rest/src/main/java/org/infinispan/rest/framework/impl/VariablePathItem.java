package org.infinispan.rest.framework.impl;

import org.infinispan.rest.framework.PathItem;

/**
 * Path item defined by an expression. The expression supports constant chars plus variable names enclosed by
 * '{' and '}'.
 *
 * A path containing {@link VariablePathItem} can match multiple paths at runtime. Examples:
 *
 * /rest/{variable} can match /rest/a, /rest/b and so on
 * /rest/{var1}_{var2} can match /rest/a_b, rest/var1_var2 but not /rest/path.
 *
 * @since 10.0
 */
class VariablePathItem extends PathItem {

   private final String expression;
   private final String normalized;

   VariablePathItem(String expression) {
      this.expression = expression;
      this.normalized = normalize(expression);
   }

   String getExpression() {
      return expression;
   }

   private static String normalize(String expression) {
      if (expression == null) return null;
      StringBuilder builder = new StringBuilder();
      boolean variable = false;
      for (char c : expression.toCharArray()) {
         if (!variable) builder.append(c);
         if (c == '{') variable = true;
         if (c == '}' && variable) {
            variable = false;
            builder.append("}");
         }
      }
      return builder.toString();
   }

   @Override
   public String toString() {
      return expression;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      VariablePathItem that = (VariablePathItem) o;

      return normalized.equals(that.normalized);
   }

   @Override
   public int hashCode() {
      return normalized.hashCode();
   }

   @Override
   public String getPath() {
      return expression;
   }
}
