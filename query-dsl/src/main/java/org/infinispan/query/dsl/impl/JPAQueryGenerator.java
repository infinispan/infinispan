package org.infinispan.query.dsl.impl;

import org.infinispan.query.dsl.Expression;
import org.infinispan.query.dsl.Query;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.TimeZone;

/**
 * Generates a JPA query to satisfy the condition created with the builder.
 * TODO This class is not immutable and thread safe.
 *
 * @author anistor@redhat.com
 * @since 6.0
 */
public class JPAQueryGenerator implements Visitor<String> {

   private static final TimeZone GMT_TZ = TimeZone.getTimeZone("GMT");

   private static final String alias = "_gen0";

   private DateFormat dateFormat;

   public JPAQueryGenerator() {
   }

   @Override
   public <T extends Query> String visit(BaseQueryBuilder<T> baseQueryBuilder) {
      StringBuilder sb = new StringBuilder();

      if (baseQueryBuilder.getProjection() != null && baseQueryBuilder.getProjection().length != 0) {
         sb.append("SELECT ");
         boolean isFirst = true;
         for (Expression projection : baseQueryBuilder.getProjection()) {
            if (isFirst) {
               isFirst = false;
            } else {
               sb.append(", ");
            }
            appendAttributePath(sb, projection);
         }
         sb.append(' ');
      }

      sb.append("FROM ").append(baseQueryBuilder.getRootTypeName()).append(' ').append(alias);

      if (baseQueryBuilder.getWhereFilterCondition() != null) {
         BaseCondition baseCondition = baseQueryBuilder.getWhereFilterCondition().getRoot();
         String whereCondition = baseCondition.accept(this);
         if (!whereCondition.isEmpty()) {
            sb.append(" WHERE ").append(whereCondition);
         }
      }

      if (baseQueryBuilder.getGroupBy() != null && baseQueryBuilder.getGroupBy().length != 0) {
         sb.append(" GROUP BY ");
         boolean isFirst = true;
         for (String groupBy : baseQueryBuilder.getGroupBy()) {
            if (isFirst) {
               isFirst = false;
            } else {
               sb.append(", ");
            }
            sb.append(alias).append('.').append(groupBy);
         }
         sb.append(' ');
      }

      if (baseQueryBuilder.getHavingFilterCondition() != null) {
         BaseCondition baseCondition = baseQueryBuilder.getHavingFilterCondition().getRoot();
         String havingCondition = baseCondition.accept(this);
         if (!havingCondition.isEmpty()) {
            sb.append(" HAVING ").append(havingCondition);
         }
      }

      if (baseQueryBuilder.getSortCriteria() != null && !baseQueryBuilder.getSortCriteria().isEmpty()) {
         sb.append(" ORDER BY ");
         boolean isFirst = true;
         for (SortCriteria sortCriteria : baseQueryBuilder.getSortCriteria()) {
            if (isFirst) {
               isFirst = false;
            } else {
               sb.append(", ");
            }
            sb.append(alias).append('.').append(sortCriteria.getAttributePath()).append(' ').append(sortCriteria.getSortOrder().name());
         }
      }

      return sb.toString();
   }

   protected <E extends Enum<E>> String renderEnum(E argument) {
      return '\'' + argument.name() + '\'';
   }

   protected String renderBoolean(boolean argument) {
      return argument ? "true" : "false";
   }

   @Override
   public String visit(AndCondition booleanCondition) {
      StringBuilder sb = new StringBuilder();
      sb.append("(");
      sb.append(booleanCondition.getFirstCondition().accept(this));
      sb.append(") AND ( ");
      sb.append(booleanCondition.getSecondCondition().accept(this));
      sb.append(")");
      return sb.toString();
   }

   @Override
   public String visit(OrCondition booleanCondition) {
      StringBuilder sb = new StringBuilder();
      sb.append("(");
      sb.append(booleanCondition.getFirstCondition().accept(this));
      sb.append(") OR ( ");
      sb.append(booleanCondition.getSecondCondition().accept(this));
      sb.append(")");
      return sb.toString();
   }

   @Override
   public String visit(NotCondition notCondition) {
      StringBuilder sb = new StringBuilder();
      sb.append("NOT (");
      sb.append(notCondition.getFirstCondition().accept(this));
      sb.append(")");
      return sb.toString();
   }

   @Override
   public String visit(EqOperator operator) {
      return appendSingleCondition(new StringBuilder(), operator.getAttributeCondition(), operator.getArgument(), "=", "!=").toString();
   }

   @Override
   public String visit(GtOperator operator) {
      return appendSingleCondition(new StringBuilder(), operator.getAttributeCondition(), operator.getArgument(), ">", "<=").toString();
   }

   @Override
   public String visit(GteOperator operator) {
      return appendSingleCondition(new StringBuilder(), operator.getAttributeCondition(), operator.getArgument(), ">=", "<").toString();
   }

   @Override
   public String visit(LtOperator operator) {
      return appendSingleCondition(new StringBuilder(), operator.getAttributeCondition(), operator.getArgument(), "<", ">=").toString();
   }

   @Override
   public String visit(LteOperator operator) {
      return appendSingleCondition(new StringBuilder(), operator.getAttributeCondition(), operator.getArgument(), "<=", ">").toString();
   }

   @Override
   public String visit(BetweenOperator operator) {
      StringBuilder sb = new StringBuilder();
      ValueRange range = operator.getArgument();
      if (!range.isIncludeLower() || !range.isIncludeUpper()) {
         appendAttributePath(sb, operator.getAttributeCondition());
         sb.append(operator.getAttributeCondition().isNegated() ?
                         (range.isIncludeLower() ? " < " : " <= ") : (range.isIncludeLower() ? " >= " : " > "));
         appendArgument(sb, range.getFrom());
         sb.append(operator.getAttributeCondition().isNegated() ?
                         " OR " : " AND ");
         appendAttributePath(sb, operator.getAttributeCondition());
         sb.append(operator.getAttributeCondition().isNegated() ?
                         (range.isIncludeUpper() ? " > " : " >= ") : (range.isIncludeUpper() ? " <= " : " < "));
         appendArgument(sb, range.getTo());
      } else {
         if (operator.getAttributeCondition().isNegated()) {
            sb.append("NOT ");
         }
         appendAttributePath(sb, operator.getAttributeCondition());
         sb.append(" BETWEEN ");
         appendArgument(sb, range.getFrom());
         sb.append(" AND ");
         appendArgument(sb, range.getTo());
      }
      return sb.toString();
   }

   @Override
   public String visit(LikeOperator operator) {
      StringBuilder sb = new StringBuilder();
      appendAttributePath(sb, operator.getAttributeCondition());
      sb.append(' ');
      if (operator.getAttributeCondition().isNegated()) {
         sb.append("NOT ");
      }
      sb.append("LIKE ");
      appendArgument(sb, operator.getArgument());
      return sb.toString();
   }

   @Override
   public String visit(IsNullOperator operator) {
      StringBuilder sb = new StringBuilder();
      appendAttributePath(sb, operator.getAttributeCondition());
      sb.append(" IS ");
      if (operator.getAttributeCondition().isNegated()) {
         sb.append("NOT ");
      }
      sb.append("null");    //TODO HQL parser chokes on 'NULL' but 'null' is fine. definitely a grammar bug.
      return sb.toString();
   }

   @Override
   public String visit(InOperator operator) {
      StringBuilder sb = new StringBuilder();
      appendAttributePath(sb, operator.getAttributeCondition());
      if (operator.getAttributeCondition().isNegated()) {
         sb.append(" NOT");
      }
      sb.append(" IN ");
      appendArgument(sb, operator.getArgument());
      return sb.toString();
   }

   @Override
   public String visit(ContainsOperator operator) {
      return appendSingleCondition(new StringBuilder(), operator.getAttributeCondition(), operator.getArgument(), "=", "!=").toString();
   }

   private StringBuilder appendSingleCondition(StringBuilder sb, AttributeCondition attributeCondition, Object argument, String op, String negativeOp) {
      appendAttributePath(sb, attributeCondition);
      sb.append(' ');
      sb.append(attributeCondition.isNegated() ? negativeOp : op);
      sb.append(' ');
      appendArgument(sb, argument);
      return sb;
   }

   @Override
   public String visit(ContainsAllOperator operator) {
      return generateMultipleCondition(operator, "AND");
   }

   @Override
   public String visit(ContainsAnyOperator operator) {
      return generateMultipleCondition(operator, "OR");
   }

   private String generateMultipleCondition(OperatorAndArgument operator, String booleanOperator) {
      Object argument = operator.getArgument();
      Collection values;
      if (argument instanceof Collection) {
         values = (Collection) argument;
      } else if (argument instanceof Object[]) {
         values = Arrays.asList((Object[]) argument);
      } else {
         throw new IllegalArgumentException("Expecting a Collection or an array of Object");
      }
      StringBuilder sb = new StringBuilder();
      boolean isFirst = true;
      for (Object value : values) {
         if (isFirst) {
            isFirst = false;
         } else {
            sb.append(' ').append(booleanOperator).append(' ');
         }
         appendSingleCondition(sb, operator.getAttributeCondition(), value, "=", "!=");
      }
      return sb.toString();
   }

   @Override
   public String visit(AttributeCondition attributeCondition) {
      if (attributeCondition.getExpression() == null || attributeCondition.getOperatorAndArgument() == null) {
         throw new IllegalStateException("Incomplete sentence. Missing attribute path or operator.");
      }

      return attributeCondition.getOperatorAndArgument().accept(this);
   }

   private void appendAttributePath(StringBuilder sb, AttributeCondition attributeCondition) {
      appendAttributePath(sb, attributeCondition.getExpression());
   }

   private void appendAttributePath(StringBuilder sb, Expression expression) {
      PathExpression pathExpression = (PathExpression) expression;
      if (pathExpression.getAggregationType() != null) {
         sb.append(pathExpression.getAggregationType().name()).append('(');
      }
      sb.append(alias).append('.').append(pathExpression.getPath());
      if (pathExpression.getAggregationType() != null) {
         sb.append(')');
      }
   }

   private void appendArgument(StringBuilder sb, Object argument) {
      if (argument instanceof String) {
         sb.append('\'');
         String stringLiteral = argument.toString();
         for (int i = 0; i < stringLiteral.length(); i++) {
            char c = stringLiteral.charAt(i);
            if (c == '\'') {
               sb.append('\'');
            }
            sb.append(c);
         }
         sb.append('\'');
         return;
      }

      if (argument instanceof Number) {
         sb.append(argument);
         return;
      }

      if (argument instanceof Boolean) {
         sb.append(renderBoolean((Boolean) argument));
         return;
      }

      if (argument instanceof Enum) {
         sb.append(renderEnum((Enum) argument));
         return;
      }

      if (argument instanceof Collection) {
         sb.append('(');
         boolean isFirstElement = true;
         for (Object o : (Collection) argument) {
            if (isFirstElement) {
               isFirstElement = false;
            } else {
               sb.append(", ");
            }
            appendArgument(sb, o);
         }
         sb.append(')');
         return;
      }

      if (argument instanceof Object[]) {
         sb.append('(');
         boolean isFirstElement = true;
         for (Object o : (Object[]) argument) {
            if (isFirstElement) {
               isFirstElement = false;
            } else {
               sb.append(", ");
            }
            appendArgument(sb, o);
         }
         sb.append(')');
         return;
      }

      if (argument instanceof Date) {
         sb.append('\'').append(renderDate((Date) argument)).append('\'');
         return;
      }

      sb.append(argument);
   }

   protected String renderDate(Date argument) {
      return getDateFormatter().format(argument);
   }

   private DateFormat getDateFormatter() {
      if (dateFormat == null) {
         dateFormat = new SimpleDateFormat("yyyyMMddHHmmssSSS");
         dateFormat.setTimeZone(GMT_TZ);
      }
      return dateFormat;
   }
}
