package org.infinispan.objectfilter.impl.syntax;

import org.infinispan.objectfilter.PropertyPath;
import org.infinispan.objectfilter.SortField;

/**
 * Generates a JPA query from an expression tree.
 *
 * @author anistor@redhat.com
 * @since 8.0
 */
public final class JPATreePrinter {

   private JPATreePrinter() {
   }

   public static String printTree(BooleanExpr whereClause) {
      StringBuilder sb = new StringBuilder();
      if (whereClause != null) {
         if (whereClause == ConstantBooleanExpr.FALSE) {
            throw new IllegalArgumentException("The WHERE clause must not be a contradiction");
         }
         if (whereClause != ConstantBooleanExpr.TRUE) {
            sb.append(" WHERE ").append(whereClause.toQueryString());
         }
      }
      return sb.toString();
   }

   public static String printTree(String fromEntityTypeName, PropertyPath[] projection, BooleanExpr whereClause, SortField[] orderBy) {
      StringBuilder sb = new StringBuilder();

      if (projection != null && projection.length != 0) {
         sb.append("SELECT ");
         boolean isFirst = true;
         for (PropertyPath p : projection) {
            if (isFirst) {
               isFirst = false;
            } else {
               sb.append(", ");
            }
            if (p.getAggregationType() != null) {
               sb.append(p.getAggregationType().name()).append('(');
            }
            sb.append(p.asStringPath());
            if (p.getAggregationType() != null) {
               sb.append(')');
            }
         }
         sb.append(' ');
      }

      sb.append("FROM ").append(fromEntityTypeName);
      sb.append(printTree(whereClause));
      if (orderBy != null && orderBy.length != 0) {
         sb.append(" ORDER BY ");
         for (int i = 0; i < orderBy.length; i++) {
            if (i != 0) {
               sb.append(", ");
            }
            SortField sf = orderBy[i];
            sb.append(sf.getPath());
            if (!sf.isAscending()) {
               sb.append(" DESC");
            }
         }
      }
      return sb.toString();
   }
}
