package org.infinispan.query.dsl.impl;

import org.infinispan.query.dsl.Query;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.TimeZone;

/**
 * Generates a JPA query to satisfy the condition created with the builder.
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
         for (String projection : baseQueryBuilder.getProjection()) {
            if (isFirst) {
               isFirst = false;
            } else {
               sb.append(", ");
            }
            sb.append(alias).append('.').append(projection);
         }
         sb.append(' ');
      }

      sb.append("FROM ").append(renderEntityName(baseQueryBuilder.getRootType())).append(' ').append(alias);

      if (baseQueryBuilder.getFilterCondition() != null) {
         BaseCondition baseCondition = baseQueryBuilder.getFilterCondition().getRoot();
         String whereCondition = baseCondition.accept(this);
         if (!whereCondition.isEmpty()) {
            sb.append(" WHERE ").append(whereCondition);
         }
      }

      //TODO the 'ORDER BY' clause is ignored by HQL parser anyway, see https://hibernate.atlassian.net/browse/HQLPARSER-24
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

   protected String renderEntityName(Class<?> rootType) {
      return rootType.getName();
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
      if (attributeCondition.getAttributePath() == null || attributeCondition.getOperatorAndArgument() == null) {
         throw new IllegalStateException("Incomplete sentence. Missing attribute path or operator.");
      }

      return attributeCondition.getOperatorAndArgument().accept(this);
   }

   private void appendAttributePath(StringBuilder sb, AttributeCondition attributeCondition) {
      sb.append(alias).append('.').append(attributeCondition.getAttributePath());
   }

   private void appendArgument(StringBuilder sb, Object argument) {
      if (argument instanceof String) {
         sb.append('\'').append(argument).append('\''); //todo [anistor] need to ensure proper string escaping. this is just a dummy attempt
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
         sb.append('\'').append(getDateFormatter().format(argument)).append('\'');
         return;
      }

      sb.append(argument);
   }

   private DateFormat getDateFormatter() {
      if (dateFormat == null) {
         dateFormat = new SimpleDateFormat("yyyyMMddHHmmssSSS");
         dateFormat.setTimeZone(GMT_TZ);
      }
      return dateFormat;
   }
}
