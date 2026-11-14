package org.infinispan.query.objectfilter.impl.syntax.update;

import java.util.Arrays;

import org.infinispan.query.functions.QueryFunctionContext;

/**
 * A reference to the current value of a property of the target entity in an
 * UPDATE statement value expression, e.g. the {@code balance} in
 * {@code set balance = balance + 1}. The property is validated against the
 * entity metadata at parse time.
 *
 * @since 16.4
 */
public final class PropertyRefUpdateValue implements UpdateValueExpr {

   private final String[] path;

   public PropertyRefUpdateValue(String[] path) {
      if (path == null || path.length == 0) {
         throw new IllegalArgumentException("path cannot be null or empty");
      }
      this.path = path.clone();
   }

   public String[] getPath() {
      return path.clone();
   }

   @Override
   public Object evaluate(QueryFunctionContext ctx) {
      return ctx.getPropertyValue(path);
   }

   @Override
   public void appendQueryString(StringBuilder sb) {
      sb.append(String.join(".", path));
   }

   @Override
   public boolean equals(Object o) {
      return o != null && o.getClass() == PropertyRefUpdateValue.class && Arrays.equals(path, ((PropertyRefUpdateValue) o).path);
   }

   @Override
   public int hashCode() {
      return Arrays.hashCode(path);
   }
}
