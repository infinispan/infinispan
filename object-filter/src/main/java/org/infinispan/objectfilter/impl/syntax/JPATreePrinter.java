package org.infinispan.objectfilter.impl.syntax;

import org.infinispan.objectfilter.SortField;

import java.util.List;

/**
 * Generates a JPA query from an expression tree.
 *
 * @author anistor@redhat.com
 * @since 8.0
 */
public final class JPATreePrinter {

   public static String printTree(String entityTypeName, BooleanExpr query, List<SortField> sortFields) {
      StringBuilder sb = new StringBuilder();
      sb.append("FROM ").append(entityTypeName);
      if (query != ConstantBooleanExpr.TRUE) {
         sb.append(" WHERE ").append(query.toJpaString());
      }
      if (sortFields != null && !sortFields.isEmpty()) {
         sb.append(" ORDER BY ");
         for (int i = 0; i < sortFields.size(); i++) {
            if (i != 0) {
               sb.append(", ");
            }
            SortField sf = sortFields.get(i);
            sb.append(sf.getPath());
            if (!sf.isAscending()) {
               sb.append(" DESC");
            }
         }
      }
      return sb.toString();
   }
}
