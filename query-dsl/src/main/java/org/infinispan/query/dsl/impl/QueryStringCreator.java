package org.infinispan.query.dsl.impl;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import org.infinispan.query.dsl.Expression;
import org.infinispan.query.dsl.impl.logging.Log;
import org.jboss.logging.Logger;

/**
 * Generates an Ickle query to satisfy the condition created with the builder.
 *
 * @author anistor@redhat.com
 * @since 6.0
 */
public class QueryStringCreator implements Visitor<String> {

   private static final Log log = Logger.getMessageLogger(Log.class, QueryStringCreator.class.getName());

   public static final String DEFAULT_ALIAS = "_gen0";

   private static final String DATE_FORMAT = "yyyyMMddHHmmssSSS";

   private static final TimeZone GMT_TZ = TimeZone.getTimeZone("GMT");

   protected Map<String, Object> namedParameters;

   private DateFormat dateFormat;

   public QueryStringCreator() {
   }

   public Map<String, Object> getNamedParameters() {
      return namedParameters;
   }

   @Override
   public String visit(BaseQueryBuilder baseQueryBuilder) {
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

      sb.append("FROM ").append(baseQueryBuilder.getRootTypeName()).append(' ').append(DEFAULT_ALIAS);

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
            sb.append(DEFAULT_ALIAS).append('.').append(groupBy);
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
            appendAttributePath(sb, sortCriteria.getAttributePath());
            sb.append(' ').append(sortCriteria.getSortOrder().name());
         }
      }

      return sb.toString();
   }

   protected <E extends Enum<E>> String renderEnum(E argument) {
      return '\'' + argument.name() + '\'';
   }

   @Override
   public String visit(AndCondition booleanCondition) {
      return generateBooleanCondition(booleanCondition, "AND");
   }

   @Override
   public String visit(OrCondition booleanCondition) {
      return generateBooleanCondition(booleanCondition, "OR");
   }

   private String generateBooleanCondition(BooleanCondition booleanCondition, String booleanOperator) {
      StringBuilder sb = new StringBuilder();
      boolean wrap = parentIsNotOfClass(booleanCondition, booleanCondition.getClass());
      if (wrap) {
         sb.append('(');
      }
      sb.append(booleanCondition.getFirstCondition().accept(this));
      sb.append(' ').append(booleanOperator).append(' ');
      sb.append(booleanCondition.getSecondCondition().accept(this));
      if (wrap) {
         sb.append(')');
      }
      return sb.toString();
   }

   @Override
   public String visit(NotCondition notCondition) {
      return "NOT " + notCondition.getFirstCondition().accept(this);
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
         // if any of the bounds are not included then we cannot use BETWEEN and need to resort to simple comparisons
         boolean wrap = parentIsNotOfClass(operator.getAttributeCondition(), operator.getAttributeCondition().isNegated() ? OrCondition.class : AndCondition.class);
         if (wrap) {
            sb.append('(');
         }
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
         if (wrap) {
            sb.append(')');
         }
      } else {
         // if the bounds are included then we can use BETWEEN
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
      sb.append("NULL");
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

   /**
    * We check if the parent if of the expected class, hoping that if it is then we can avoid wrapping this condition in
    * parentheses and still maintain the same logic.
    *
    * @return {@code true} if wrapping is needed, {@code false} otherwise
    */
   private boolean parentIsNotOfClass(BaseCondition condition, Class<? extends BooleanCondition> expectedParentClass) {
      BaseCondition parent = condition.getParent();
      return parent != null && parent.getClass() != expectedParentClass;
   }

   @Override
   public String visit(ContainsAllOperator operator) {
      return generateMultipleBooleanCondition(operator, "AND", AndCondition.class);
   }

   @Override
   public String visit(ContainsAnyOperator operator) {
      return generateMultipleBooleanCondition(operator, "OR", OrCondition.class);
   }

   private String generateMultipleBooleanCondition(OperatorAndArgument operator, String booleanOperator, Class<? extends BooleanCondition> expectedParentClass) {
      Object argument = operator.getArgument();
      Collection values;
      if (argument instanceof Collection) {
         values = (Collection) argument;
      } else if (argument instanceof Object[]) {
         values = Arrays.asList((Object[]) argument);
      } else {
         throw log.expectingCollectionOrArray();
      }
      StringBuilder sb = new StringBuilder();
      boolean wrap = parentIsNotOfClass(operator.getAttributeCondition(), expectedParentClass);
      if (wrap) {
         sb.append('(');
      }
      boolean isFirst = true;
      for (Object value : values) {
         if (isFirst) {
            isFirst = false;
         } else {
            sb.append(' ').append(booleanOperator).append(' ');
         }
         appendSingleCondition(sb, operator.getAttributeCondition(), value, "=", "!=");
      }
      if (wrap) {
         sb.append(')');
      }
      return sb.toString();
   }

   @Override
   public String visit(AttributeCondition attributeCondition) {
      if (attributeCondition.getExpression() == null || attributeCondition.getOperatorAndArgument() == null) {
         throw log.incompleteSentence();
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
      sb.append(DEFAULT_ALIAS).append('.').append(pathExpression.getPath());
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

      if (argument instanceof ParameterExpression) {
         ParameterExpression param = (ParameterExpression) argument;
         sb.append(':').append(param.getParamName());
         if (namedParameters == null) {
            namedParameters = new HashMap<>(5);
         }
         namedParameters.put(param.getParamName(), null);
         return;
      }

      if (argument instanceof PathExpression) {
         appendAttributePath(sb, (PathExpression) argument);
         return;
      }

      if (argument instanceof Number) {
         sb.append(argument);
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

      if (argument instanceof Instant) {
         sb.append('\'').append(renderInstant((Instant) argument)).append('\'');
         return;
      }

      sb.append(argument);
   }

   protected String renderDate(Date argument) {
      return getDateFormatter().format(argument);
   }

   protected String renderInstant(Instant argument) {
      return argument.toString();
   }

   private DateFormat getDateFormatter() {
      if (dateFormat == null) {
         dateFormat = new SimpleDateFormat(DATE_FORMAT);
         dateFormat.setTimeZone(GMT_TZ);
      }
      return dateFormat;
   }
}
