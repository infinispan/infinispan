package org.infinispan.query.objectfilter.impl.syntax;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.infinispan.query.objectfilter.impl.syntax.parser.VirtualExpressionBuilder;

/**
 * An expression that applies a boolean operator (OR, AND) to a list of boolean sub-expressions.
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
public abstract class BooleanOperatorExpr implements BooleanExpr {

   protected final List<BooleanExpr> children = new ArrayList<>();

   protected BooleanOperatorExpr(BooleanExpr... children) {
      Collections.addAll(this.children, children);
   }

   protected BooleanOperatorExpr(List<BooleanExpr> children) {
      this.children.addAll(children);
   }

   public List<BooleanExpr> getChildren() {
      return children;
   }

   /**
    * Extracts the field name from a full-text expression, unwrapping
    * FullTextOccurExpr and FullTextBoostExpr layers.
    * Returns null if not a full-text expression.
    */
   protected static String getFullTextFieldName(BooleanExpr expr) {
      if (expr instanceof FullTextOccurExpr) {
         expr = ((FullTextOccurExpr) expr).getChild();
      }
      if (expr instanceof FullTextBoostExpr) {
         expr = ((FullTextBoostExpr) expr).getChild();
      }
      if (expr instanceof FullTextTermExpr) {
         return ((FullTextTermExpr) expr).getChild().toQueryString();
      }
      if (expr instanceof FullTextRangeExpr) {
         return ((FullTextRangeExpr) expr).getChild().toQueryString();
      }
      if (expr instanceof FullTextRegexpExpr) {
         return ((FullTextRegexpExpr) expr).getChild().toQueryString();
      }
      return null;
   }

   /**
    * Checks if ALL children are full-text expressions on the same field.
    * Returns the common field name, or null otherwise.
    */
   protected String getCommonFullTextField() {
      if (children.isEmpty()) return null;
      String commonField = null;
      for (BooleanExpr child : children) {
         String field = getFullTextFieldName(child);
         if (field == null) return null;
         if (commonField == null) {
            commonField = field;
         } else if (!commonField.equals(field)) {
            return null;
         }
      }
      return commonField;
   }

   /**
    * Appends the inner part of a full-text expression WITHOUT the field name.
    * E.g. for MUST(longDescription:'beer'), appends +'beer' instead of +longDescription:'beer'
    */
   protected static void appendFullTextInner(StringBuilder sb, BooleanExpr expr) {
      if (expr instanceof FullTextOccurExpr) {
         FullTextOccurExpr occurExpr = (FullTextOccurExpr) expr;
         sb.append(((VirtualExpressionBuilder.Occur) occurExpr.getOccur()).getOperator());
         appendFullTextInner(sb, occurExpr.getChild());
      } else if (expr instanceof FullTextBoostExpr) {
         FullTextBoostExpr boostExpr = (FullTextBoostExpr) expr;
         appendFullTextInner(sb, boostExpr.getChild());
         sb.append("^").append(boostExpr.getBoost());
      } else if (expr instanceof FullTextTermExpr) {
         FullTextTermExpr termExpr = (FullTextTermExpr) expr;
         sb.append("'").append(termExpr.getTerm(null)).append("'");
         if (termExpr.getFuzzySlop() != null) {
            sb.append("~").append(termExpr.getFuzzySlop());
         }
      } else if (expr instanceof FullTextRangeExpr) {
         FullTextRangeExpr rangeExpr = (FullTextRangeExpr) expr;
         sb.append(rangeExpr.isIncludeLower() ? '[' : '{');
         sb.append(rangeExpr.getLower() == null ? "*" : rangeExpr.getLower());
         sb.append(" TO ");
         sb.append(rangeExpr.getUpper() == null ? "*" : rangeExpr.getUpper());
         sb.append(rangeExpr.isIncludeUpper() ? ']' : '}');
      } else if (expr instanceof FullTextRegexpExpr) {
         FullTextRegexpExpr regexpExpr = (FullTextRegexpExpr) expr;
         sb.append("/").append(regexpExpr.getRegexp()).append("/");
      } else {
         expr.appendQueryString(sb);
      }
   }
}
