package org.infinispan.query.objectfilter.impl.syntax.update;

import org.infinispan.query.functions.QueryFunctionContext;
import org.infinispan.query.objectfilter.impl.syntax.ConstantValueExpr;

/**
 * A constant literal in an UPDATE statement value expression. Unlike
 * {@link ConstantValueExpr}, a {@code null} value is allowed.
 *
 * @since 16.4
 */
public final class ConstantUpdateValue implements UpdateValueExpr {

   private final Object value;

   public ConstantUpdateValue(Object value) {
      this.value = value;
   }

   public Object getValue() {
      return value;
   }

   @Override
   public Object evaluate(QueryFunctionContext ctx) {
      return value;
   }

   @Override
   public void appendQueryString(StringBuilder sb) {
      if (value == null) {
         sb.append("null");
      } else if (value instanceof String s) {
         // Escape embedded single quotes with the doubled-quote convention
         // used by the lexer (STRING_LITERAL) so the output stays valid Ickle.
         sb.append('\'').append(s.replace("'", "''")).append('\'');
      } else {
         sb.append(value);
      }
   }

   @Override
   public boolean equals(Object o) {
      if (o == null || o.getClass() != ConstantUpdateValue.class) {
         return false;
      }
      Object other = ((ConstantUpdateValue) o).value;
      return value == null ? other == null : value.equals(other);
   }

   @Override
   public int hashCode() {
      return value != null ? value.hashCode() : 0;
   }
}
