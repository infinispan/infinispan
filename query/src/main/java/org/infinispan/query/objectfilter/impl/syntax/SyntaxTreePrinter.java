package org.infinispan.query.objectfilter.impl.syntax;

import org.infinispan.query.objectfilter.SortField;
import org.infinispan.query.objectfilter.impl.ql.PropertyPath;

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
      return printTree(whereClause, " WHERE ");
   }

   public static String printTree(BooleanExpr clause, String clauseName) {
      StringBuilder sb = new StringBuilder();
      if (clause != null) {
         if (clause == ConstantBooleanExpr.FALSE) {
            throw new IllegalArgumentException("The clause must not be a contradiction");
         }
         if (clause != ConstantBooleanExpr.TRUE) {
            sb.append(clauseName);
            clause.appendQueryString(sb);
         }
      }
      return sb.toString();
   }

   public static String printTree(String fromEntityTypeName, PropertyPath[] projection, BooleanExpr whereClause, BooleanExpr filtering, SortField[] orderBy) {
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

      if (filtering != null) {
         sb.append(printTree(filtering, " FILTERING "));
      }

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
