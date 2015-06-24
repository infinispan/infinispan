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

   public static String printTree(BooleanExpr whereClause) {
      StringBuilder sb = new StringBuilder();
      if (whereClause != null && whereClause != ConstantBooleanExpr.TRUE) {
         sb.append(" WHERE ").append(whereClause.toJpaString());
      }
      return sb.toString();
   }

   public static String printTree(String fromEntityTypeName, BooleanExpr whereClause, List<SortField> orderBy) {
      StringBuilder sb = new StringBuilder();
      sb.append("FROM ").append(fromEntityTypeName);
      sb.append(printTree(whereClause));
      if (orderBy != null && !orderBy.isEmpty()) {
         sb.append(" ORDER BY ");
         for (int i = 0; i < orderBy.size(); i++) {
            if (i != 0) {
               sb.append(", ");
            }
            SortField sf = orderBy.get(i);
            sb.append(sf.getPath());
            if (!sf.isAscending()) {
               sb.append(" DESC");
            }
         }
      }
      return sb.toString();
   }
}
