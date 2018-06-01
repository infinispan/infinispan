package org.infinispan.objectfilter.impl.syntax;

import org.infinispan.objectfilter.SortField;
import org.infinispan.objectfilter.impl.ql.PropertyPath;

/**
 * Generates an Ickle query from an expression tree.
 *
 * @author anistor@redhat.com
 * @since 8.0
 */
public final class SyntaxTreePrinter {

   private SyntaxTreePrinter() {
   }

   public static String printTree(BooleanExpr whereClause) {
      StringBuilder sb = new StringBuilder();
      if (whereClause != null) {
         if (whereClause == ConstantBooleanExpr.FALSE) {
            throw new IllegalArgumentException("The WHERE clause must not be a boolean contradiction");
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
         for (int i = 0; i < projection.length; i++) {
            if (i != 0) {
               sb.append(", ");
            }
            sb.append(projection[i]);
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
