package org.infinispan.query.dsl.impl;

import java.util.Collection;

/**
 * Generates a JPA query to satisfy the condition built by the builder.
 *
 * @author anistor@redhat.com
 * @since 6.0
 */
class JPAQueryGeneratorVisitor implements Visitor<String> {

   private final String alias = "_gen0";

   public JPAQueryGeneratorVisitor() {
   }

   @Override
   public String visit(CompositeCondition compositeCondition) {
      StringBuilder sb = new StringBuilder();
      sb.append("(");
      sb.append(compositeCondition.getLeftCondition().accept(this));
      sb.append(") ");
      sb.append(compositeCondition.isConjunction() ? "AND" : "OR");
      sb.append(" (");
      sb.append(compositeCondition.getRightCondition().accept(this));
      sb.append(")");
      return sb.toString();
   }

   @Override
   public String visit(LuceneQueryBuilder luceneQueryBuilder) {
      StringBuilder sb = new StringBuilder();
      sb.append("FROM ").append(luceneQueryBuilder.getRootType().getName()).append(" ").append(alias);
      if (luceneQueryBuilder.getFilterCondition() != null) {
         BaseCondition baseCondition = luceneQueryBuilder.getFilterCondition().getRoot();
         sb.append(" WHERE ").append(baseCondition.accept(this));
      }
      return sb.toString();
   }

   @Override
   public String visit(AttributeCondition attributeCondition) {
      if (attributeCondition.getAttributePath() == null || attributeCondition.getOperator() == null) {
         throw new IllegalStateException("Incomplete sentence. Missing attribute path or operator.");
      }
      //todo validate argument type is ok for the operator
      StringBuilder sb = new StringBuilder();

      if (attributeCondition.getOperator() == AttributeCondition.Operator.IS_NULL) {
         sb.append(alias).append(".").append(attributeCondition.getAttributePath());
         sb.append(" IS ");
         if (attributeCondition.isNegated()) {
            sb.append("NOT ");
         }
         sb.append("NULL");
      } else if (attributeCondition.getOperator() == AttributeCondition.Operator.BETWEEN) {
         ValueRange range = (ValueRange) attributeCondition.getArgument();
         if (attributeCondition.isNegated() || !range.isIncludeLower() || !range.isIncludeUpper()) {
            sb.append(alias).append(".").append(attributeCondition.getAttributePath());
            if (attributeCondition.isNegated()) {
               sb.append(range.isIncludeLower() ? " < " : " <= ");
            } else {
               sb.append(range.isIncludeLower() ? " >= " : " > ");
            }
            sb.append(getArgumentLiteral(range.getFrom()));
            sb.append(" AND ");
            sb.append(alias).append(".").append(attributeCondition.getAttributePath());
            if (attributeCondition.isNegated()) {
               sb.append(range.isIncludeUpper() ? " > " : " >= ");
            } else {
               sb.append(range.isIncludeUpper() ? " <= " : " < ");
            }
            sb.append(getArgumentLiteral(range.getTo()));
         } else {
            sb.append(alias).append(".").append(attributeCondition.getAttributePath());
            sb.append(" BETWEEN ");
            sb.append(getArgumentLiteral(range.getFrom()));
            sb.append(" AND ");
            sb.append(getArgumentLiteral(range.getTo()));
         }
      } else {
         sb.append(alias).append(".").append(attributeCondition.getAttributePath());
         sb.append(' ');
         switch (attributeCondition.getOperator()) {
            case EQ: {
               sb.append(attributeCondition.isNegated() ? "!=" : "=");
               break;
            }
            case LT: {
               sb.append(attributeCondition.isNegated() ? ">=" : "<");
               break;
            }
            case LTE: {
               sb.append(attributeCondition.isNegated() ? ">" : "<=");
               break;
            }
            case GT: {
               sb.append(attributeCondition.isNegated() ? "<=" : ">");
               break;
            }
            case GTE: {
               sb.append(attributeCondition.isNegated() ? "<" : ">=");
               break;
            }
            case IN: {
               sb.append(attributeCondition.isNegated() ? "NOT IN" : "IN");
               break;
            }
            case LIKE: {
               sb.append(attributeCondition.isNegated() ? "NOT LIKE" : "LIKE");
               break;
            }
         }
         sb.append(' ');
         sb.append(getArgumentLiteral(attributeCondition.getArgument()));
      }

      return sb.toString();
   }

   private String getArgumentLiteral(Object argument) {
      if (argument instanceof String) {
         return "'" + argument + "'"; //todo [anistor] need to ensure proper string escaping. this is just a dummy attempt
      }

      if (argument instanceof Number || argument instanceof Boolean) {
         return argument.toString();
      }

      if (argument instanceof Collection) {
         StringBuilder sb = new StringBuilder();
         sb.append("(");
         boolean isFirstElement = true;
         for (Object o : (Collection) argument) {
            if (isFirstElement) {
               isFirstElement = false;
            } else {
               sb.append(", ");
            }
            sb.append(getArgumentLiteral(o));
         }
         sb.append(")");
         return sb.toString();
      }

      if (argument instanceof Object[]) {
         StringBuilder sb = new StringBuilder();
         sb.append("(");
         boolean isFirstElement = true;
         for (Object o : (Object[]) argument) {
            if (isFirstElement) {
               isFirstElement = false;
            } else {
               sb.append(", ");
            }
            sb.append(getArgumentLiteral(o));
         }
         sb.append(")");
         return sb.toString();
      }

      return argument.toString();
   }
}
