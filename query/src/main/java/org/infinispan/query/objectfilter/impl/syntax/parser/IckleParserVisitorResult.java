package org.infinispan.query.objectfilter.impl.syntax.parser;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.antlr.v4.runtime.tree.ParseTree;
import org.infinispan.query.grammar.IckleLexer;
import org.infinispan.query.grammar.IckleParser;
import org.infinispan.query.grammar.IckleParserBaseVisitor;
import org.infinispan.query.objectfilter.SortField;
import org.infinispan.query.objectfilter.impl.logging.Log;
import org.infinispan.query.objectfilter.impl.ql.AggregationFunction;
import org.infinispan.query.objectfilter.impl.ql.Function;
import org.infinispan.query.objectfilter.impl.ql.PropertyPath;
import org.infinispan.query.objectfilter.impl.syntax.ComparisonExpr;
import org.infinispan.query.objectfilter.impl.syntax.ConstantValueExpr;
import org.infinispan.query.objectfilter.impl.syntax.IndexedFieldProvider;
import org.infinispan.query.objectfilter.impl.syntax.parser.projection.CacheValuePropertyPath;
import org.infinispan.query.objectfilter.impl.syntax.parser.projection.ScorePropertyPath;
import org.infinispan.query.objectfilter.impl.syntax.parser.projection.VersionPropertyPath;
import org.jboss.logging.Logger;
import org.jspecify.annotations.Nullable;

/**
 *
 * @author Katia Aresti
 * @since 16.2
 */
public class IckleParserVisitorResult<TypeMetadata> extends IckleParserBaseVisitor
      implements VirtualExpressionBuilder.PhaseProvider {
   private static final Log log = Logger.getMessageLogger(MethodHandles.lookup(), Log.class, IckleParserVisitorResult.class.getName());
   private static final int ARRAY_INITIAL_LENGTH = 5;
   public static final String DESC = "desc";
   private final IckleParsingResult.Builder<TypeMetadata> resultBuilder;
   private final ObjectPropertyHelper<TypeMetadata> propertyHelper;

   /**
    * Persister space: keep track of aliases and entity names.
    */
   private final Map<String, String> aliasToEntityType = new HashMap<>();

   private VirtualExpressionBuilder.Phase phase;
   private boolean filtering;
   private VirtualExpressionBuilder<TypeMetadata> expressionBuilder;
   private Map<String, PropertyPath<TypeDescriptor<TypeMetadata>>> aliasToPropertyPath = new HashMap<>();
   private String targetTypeName;
   private TypeMetadata targetEntityMetadata;
   private IndexedFieldProvider.FieldIndexingMetadata fieldIndexingMetadata;
   private PropertyPath<TypeDescriptor<TypeMetadata>> currentPropertyPath;
   private AggregationPropertyPath<TypeMetadata> currentAggregationPath;
   private final Map<String, Object> namedParameters = new HashMap<>(ARRAY_INITIAL_LENGTH);
   private List<SortField> sortFields;
   private List<PropertyPath<TypeDescriptor<TypeMetadata>>> groupBy;
   private List<PropertyPath<TypeDescriptor<TypeMetadata>>> projections;
   private List<Class<?>> projectedTypes;
   private List<Object> projectedNullMarkers;

   public IckleParserVisitorResult(String ickle, ObjectPropertyHelper propertyHelper) {
      this.propertyHelper = propertyHelper;
      this.resultBuilder = new IckleParsingResult.Builder<>();
      resultBuilder.setQueryString(ickle);
      expressionBuilder = new VirtualExpressionBuilder<>(this, propertyHelper, aliasToPropertyPath);
   }

   public IckleParsingResult<TypeMetadata> getParsingResult() {
      return resultBuilder.build();
   }

   @Override
   public Void visitSelectStatement(IckleParser.SelectStatementContext ctx) {
      resultBuilder.setStatementType(IckleParsingResult.StatementType.SELECT);

      // 1. Visit FROM FIRST to set targetEntityMetadata
      if (ctx.selectFrom() != null && ctx.selectFrom().fromClause() != null) {
         visit(ctx.selectFrom().fromClause());
      }

      // 2. NOW visit SELECT - targetEntityMetadata
      if (ctx.selectFrom() != null && ctx.selectFrom().selectClause() != null) {
         phase = VirtualExpressionBuilder.Phase.SELECT;
         visit(ctx.selectFrom().selectClause());

         // Calculate types HERE while targetEntityMetadata is available
         if (projections != null) {
            projectedTypes = new ArrayList<>();
            projectedNullMarkers = new ArrayList<>();
            List<PropertyPath<TypeDescriptor<TypeMetadata>>> resolvedProjections = new ArrayList<>();

            for (PropertyPath<TypeDescriptor<TypeMetadata>> path : projections) {
               PropertyPath<TypeDescriptor<TypeMetadata>> resolvedPath;
               String stringPath = path.asStringPath();
               Class<?> type;
               Object nullMarker = null;
               if (path instanceof FunctionPropertyPath) {
                  resolvedPath = path;
                  type = Double.class;
                  nullMarker = fieldIndexingMetadata.getNullMarker(resolvedPath.asArrayPath());
               } else if (path instanceof AggregationPropertyPath) {
                  AggregationPropertyPath<TypeMetadata> aggPath = (AggregationPropertyPath<TypeMetadata>) path;
                  // count(*) or count(alias) like count(s)
                  if (isEntityAggregation(aggPath)) {
                     resolvedPath = new CacheValueAggregationPropertyPath<>();
                     type = null;
                  } else {
                     PropertyPath<TypeDescriptor<TypeMetadata>> innerPath = new PropertyPath<>(aggPath.getNodes());
                     PropertyPath<TypeDescriptor<TypeMetadata>> resolved = resolveAlias(innerPath);
                     resolvedPath = path;
                     type = propertyHelper.getPrimitivePropertyType(targetEntityMetadata, resolved.asArrayPath());
                  }
               } else if (ScorePropertyPath.SCORE_PROPERTY_NAME.equals(stringPath)
                     || VersionPropertyPath.VERSION_PROPERTY_NAME.equals(stringPath)) {
                  resolvedPath = path;
                  type = Object.class;
               } else {
                  // IMPORTANT: Resolve alias before getting type
                  PropertyPath<TypeDescriptor<TypeMetadata>> resolved = resolveAlias(path);
                  String stringPathWithoutAlias = path.asStringPathWithoutAlias();
                  if (resolved.isEmpty()
                        || (path.getLength() == 1 && path.isAlias())
                        || stringPathWithoutAlias.equals(CacheValuePropertyPath.VALUE_PROPERTY_NAME)) {
                     resolvedPath = new CacheValuePropertyPath<>();
                     type = null;
                  } else {
                     resolvedPath = resolved;
                     type = propertyHelper.getPrimitivePropertyType(targetEntityMetadata, resolvedPath.asArrayPath());
                     if (type == null) {
                        throw log.getProjectionOfCompleteEmbeddedEntitiesNotSupportedException(
                              targetTypeName, stringPathWithoutAlias);
                     }
                     nullMarker = fieldIndexingMetadata.getNullMarker(resolvedPath.asArrayPath());
                  }
               }

               resolvedProjections.add(resolvedPath);
               projectedTypes.add(type);
               projectedNullMarkers.add(nullMarker);
            }

            resultBuilder.setProjectedPaths(resolvedProjections.toArray(new PropertyPath[0]));
            resultBuilder.setProjectedTypes(projectedTypes.toArray(new Class[0]));
            resultBuilder.setProjectedNullMarkers(projectedNullMarkers.toArray());
         }

         phase = null;
      }

      // 3. Visit WHERE, GROUP BY, ORDER BY, HAVING
      if (ctx.whereClause() != null) visit(ctx.whereClause());
      // Add filtering clause to result
      resultBuilder.setFilteringClause(expressionBuilder.filteringBuilder().build());
      if (ctx.groupByClause() != null) visit(ctx.groupByClause());
      if (ctx.havingClause() != null) visit(ctx.havingClause());
      if (ctx.orderByClause() != null) visit(ctx.orderByClause());

      return null;
   }

   private boolean isEntityAggregation(AggregationPropertyPath<TypeMetadata> aggPath) {
      if (aggPath.getNodes().isEmpty()) return true; // count(*)
      if (aggPath.getNodes().size() != 1) return false;
      String name = aggPath.getNodes().get(0).getPropertyName();
      return aliasToEntityType.containsKey(name)
            || name.equals(CacheValuePropertyPath.VALUE_PROPERTY_NAME);
   }

   @Override
   public Void visitDeleteStatement(IckleParser.DeleteStatementContext ctx) {
      resultBuilder.setStatementType(IckleParsingResult.StatementType.DELETE);

      // Visit deleteClause (with from)
      if (ctx.deleteClause() != null) {
         visit(ctx.deleteClause());
      }

      // Visit WHERE clause
      if (ctx.whereClause() != null) {
         visit(ctx.whereClause());
      }

      return null;
   }

   @Override
   public Object visitMainEntityPersisterReference(IckleParser.MainEntityPersisterReferenceContext ctx) {
      this.targetTypeName = ctx.entityName().getText();
      this.targetEntityMetadata = this.propertyHelper.getEntityMetadata(this.targetTypeName);

      // Check if there's an alias
      if (ctx.aliasClause() != null) {
         String alias = ctx.aliasClause().getText();
         aliasToEntityType.put(alias, targetTypeName);
      }

      if (targetEntityMetadata == null) {
         throw log.getUnknownEntity(this.targetTypeName);
      }

      this.resultBuilder.setTargetEntityName(this.targetTypeName);
      this.resultBuilder.setTargetEntityMetadata(this.targetEntityMetadata);
      this.fieldIndexingMetadata = this.propertyHelper.getIndexedFieldProvider().get(this.targetEntityMetadata);
      this.expressionBuilder.setEntityType(this.targetEntityMetadata);

      return visitChildren(ctx);
   }

   @Override
   public Object visitWhereClause(IckleParser.WhereClauseContext ctx) {
      phase = VirtualExpressionBuilder.Phase.WHERE;

      visitChildren(ctx);

      resultBuilder.setWhereClause(expressionBuilder.whereBuilder().build());

      // Set parameter names
      if (!namedParameters.isEmpty()) {
         resultBuilder.setParameterNames(Collections.unmodifiableSet(new HashSet<>(namedParameters.keySet())));
      }

      phase = null;

      return null;
   }

   @Override
   public Object visitSelectExpression(IckleParser.SelectExpressionContext ctx) {
      if (phase != VirtualExpressionBuilder.Phase.SELECT) {
         return visitChildren(ctx);
      }

      String text = ctx.expression().getText();

      if (text.contains("(")) {
         return visitChildren(ctx); // Handled by other visit
      }

      PropertyPath<TypeDescriptor<TypeMetadata>> path = buildPropertyPath(text);

      if (projections == null) {
         projections = new ArrayList<>();
      }

      projections.add(path);

      return visitChildren(ctx);
   }

   @Override
   public Object visitGroupByClause(IckleParser.GroupByClauseContext ctx) {
      phase = VirtualExpressionBuilder.Phase.GROUP_BY;

      Object result = visitChildren(ctx);

      if (groupBy != null) {
         resultBuilder.setGroupBy(groupBy.toArray(new PropertyPath[0]));
      }

      phase = null;
      return result;
   }

   @Override
   public Object visitHavingClause(IckleParser.HavingClauseContext ctx) {
      phase = VirtualExpressionBuilder.Phase.HAVING;

      visitChildren(ctx);

      resultBuilder.setHavingClause(expressionBuilder.havingBuilder().build());
      phase = null;

      return null;
   }

   @Override
   public Object visitGroupingValue(IckleParser.GroupingValueContext ctx) {
      String pathText = ctx.additiveExpression().getText();
      PropertyPath<TypeDescriptor<TypeMetadata>> property = buildPropertyPath(pathText);
      PropertyPath<TypeDescriptor<TypeMetadata>> resolved = resolveAlias(property);

      if (groupBy == null) {
         groupBy = new ArrayList<>();
      }
      groupBy.add(resolved);

      return visitChildren(ctx);
   }

   @Override
   public Object visitOrderByClause(IckleParser.OrderByClauseContext ctx) {
      phase = VirtualExpressionBuilder.Phase.ORDER_BY;

      visitChildren(ctx);

      if (sortFields != null) {
         resultBuilder.setSortFields(sortFields.toArray(new SortField[0]));
      }

      phase = null;
      return null;
   }

   public Object visitSortSpecification(IckleParser.SortSpecificationContext ctx) {
      String pathText = ctx.sortKey().getText();

      // Check if it's an aggregation function
      PropertyPath<TypeDescriptor<TypeMetadata>> property;

      // Check if it's a distance function
      if (pathText.startsWith("distance(") || pathText.startsWith("DISTANCE(")) {
         // Parse the distance function directly
         IckleParser.DistanceFunctionContext distCtx = ctx.sortKey().additiveExpression().distanceFunction();
         if (distCtx != null) {
            String propText = distCtx.propertyReference().getText();
            PropertyPath<TypeDescriptor<TypeMetadata>> path = buildPropertyPath(propText);
            PropertyPath<TypeDescriptor<TypeMetadata>> resolved = resolveAlias(path);

            List<Object> args = new ArrayList<>();
            args.add(toDouble(distCtx.lat));
            args.add(toDouble(distCtx.lon));
            if (distCtx.distanceFunctionUnit() != null) {
               args.add(distCtx.distanceFunctionUnit().unitVal().getText());
            }

            property = new FunctionPropertyPath<>(resolved.getNodes(), Function.DISTANCE, args);
         } else {
            property = buildPropertyPath(pathText);
         }
      } else if (pathText.matches(".*\\w+\\(.*\\).*")) {
         // It's an aggregation like AVG(amount) - find it in projections
         // Search in projections for matching aggregation
         property = null;
         if (projections != null) {
            for (PropertyPath<TypeDescriptor<TypeMetadata>> proj : projections) {
               String projStr = (proj instanceof AggregationPropertyPath)
                     ? proj.toString()
                     : proj.asStringPath();
               if (projStr.equals(pathText)) {
                  property = proj;
                  break;
               }
            }
         }
         if (property == null) {
            // Fallback: create it (shouldn't happen very often)
            property = parseAggregationFromText(pathText);
            if (property == null) {
               // Fallback to simple property path
               property = buildPropertyPath(pathText);
            }
         }
      } else {
         property = buildPropertyPath(pathText);
         property = resolveAlias(property);
      }

      checkAnalyzed(property, false);

      boolean isAscending = true;
      if (ctx.orderingSpecification() != null) {
         String order = ctx.orderingSpecification().getText();
         if (DESC.equalsIgnoreCase(order)) {
            isAscending = false;
         }
      }

      if (sortFields == null) {
         sortFields = new ArrayList<>();
      }
      sortFields.add(new IckleParsingResult.SortFieldImpl<>(property, isAscending));

      return visitChildren(ctx);
   }

   @Override
   public Object visitSimpleAggFunction(IckleParser.SimpleAggFunctionContext ctx) {
      // Determine function
      AggregationFunction aggFunc = getAggregationFunction(ctx);

      if (aggFunc != null) {
         if (phase == VirtualExpressionBuilder.Phase.WHERE) {
            throw log.getNoAggregationsInWhereClauseException(aggFunc.name());
         }
         if (phase == VirtualExpressionBuilder.Phase.GROUP_BY) {
            throw log.getNoAggregationsInGroupByClauseException(aggFunc.name());
         }
      } else {
         return visitChildren(ctx);
      }

      String pathText = ctx.additiveExpression().getText();
      PropertyPath<TypeDescriptor<TypeMetadata>> path = buildPropertyPath(pathText);
      PropertyPath<TypeDescriptor<TypeMetadata>> resolvedPath = resolveAlias(path);

      AggregationPropertyPath<TypeMetadata> aggPath = new AggregationPropertyPath<>(
            aggFunc,
            resolvedPath.getNodes()
      );

      if (phase == VirtualExpressionBuilder.Phase.SELECT) {
         if (projections == null) {
            projections = new ArrayList<>();
         }
         projections.add(aggPath);
      } else if (phase == VirtualExpressionBuilder.Phase.HAVING) {
         // Store for visitRelationalTail
         currentAggregationPath = aggPath;
      }

      return null; // Don't visit children (already got what we need)
   }

   private static @Nullable AggregationFunction getAggregationFunction(IckleParser.SimpleAggFunctionContext ctx) {
      AggregationFunction aggFunc = null;
      if (ctx.sum_key() != null) {
         aggFunc = AggregationFunction.SUM;
      } else if (ctx.avg_key() != null) {
         aggFunc = AggregationFunction.AVG;
      } else if (ctx.max_key() != null) {
         aggFunc = AggregationFunction.MAX;
      } else if (ctx.min_key() != null) {
         aggFunc = AggregationFunction.MIN;
      }
      return aggFunc;
   }

   private AggregationPropertyPath<TypeMetadata> parseAggregationFromText(String pathText) {
      if (!pathText.matches(".*\\w+\\(.*\\).*")) {
         return null;
      }

      // Extrait le nom de la fonction et le path interne
      int openParen = pathText.indexOf('(');
      int closeParen = pathText.lastIndexOf(')');

      if (openParen == -1 || closeParen == -1) {
         return null;
      }

      String funcName = pathText.substring(0, openParen).trim();
      String innerPath = pathText.substring(openParen + 1, closeParen).trim();

      AggregationFunction aggFunc = switch (funcName.toUpperCase()) {
         case "MIN" -> AggregationFunction.MIN;
         case "MAX" -> AggregationFunction.MAX;
         case "AVG" -> AggregationFunction.AVG;
         case "SUM" -> AggregationFunction.SUM;
         case "COUNT" -> AggregationFunction.COUNT;
         default -> null;
      };

      if (aggFunc == null) {
         return null;
      }

      PropertyPath<TypeDescriptor<TypeMetadata>> innerProp = buildPropertyPath(innerPath);
      PropertyPath<TypeDescriptor<TypeMetadata>> resolvedInner = resolveAlias(innerProp);

      return new AggregationPropertyPath<>(aggFunc, resolvedInner.getNodes());
   }

   @Override
   public Object visitCountFunction(IckleParser.CountFunctionContext ctx) {
      if (phase != VirtualExpressionBuilder.Phase.SELECT) {
         return visitChildren(ctx);
      }

      if (ctx.countArg().ASTERISK() != null) {
         // COUNT(*) - special handling
         AggregationPropertyPath<TypeMetadata> aggPath = new CacheValueAggregationPropertyPath<>();

         if (projections == null) {
            projections = new ArrayList<>();
         }
         projections.add(aggPath);
         return null;
      }

      // COUNT(property) - get the property path
      String pathText = ctx.countArg().countFunctionArguments().getText();
      PropertyPath<TypeDescriptor<TypeMetadata>> path = buildPropertyPath(pathText);
      PropertyPath<TypeDescriptor<TypeMetadata>> resolvedPath = resolveAlias(path);

      AggregationPropertyPath<TypeMetadata> aggPath = new AggregationPropertyPath<>(
            AggregationFunction.COUNT,
            resolvedPath.getNodes()
      );

      if (projections == null) {
         projections = new ArrayList<>();
      }
      projections.add(aggPath);

      return null; // Don't visit children

   }

   @Override
   public Object visitVersionFunction(IckleParser.VersionFunctionContext ctx) {
      if (phase != VirtualExpressionBuilder.Phase.SELECT) {
         return visitChildren(ctx);
      }

      if (projections == null) {
         projections = new ArrayList<>();
      }

      projections.add(new VersionPropertyPath<>());
      return null;
   }

   @Override
   public Object visitComparisonClause(IckleParser.ComparisonClauseContext ctx) {
      if (phase == VirtualExpressionBuilder.Phase.WHERE) {
         String aggregation = findAggregation(ctx.relationalExpression());
         if (aggregation != null) {
            throw log.getNoAggregationsInWhereClauseException(aggregation);
         }
      }
      // Get the left side (property path)
      IckleParser.EqualityTailContext parent = (IckleParser.EqualityTailContext) ctx.getParent();
      IckleParser.EqualityExpressionContext eqParent = (IckleParser.EqualityExpressionContext) parent.getParent();

      String propertyPathText = eqParent.relationalExpression().getText();
      String value = ctx.relationalExpression().getText();
      PropertyPath<TypeDescriptor<TypeMetadata>> propertyPath = buildPropertyPath(propertyPathText);
      PropertyPath<TypeDescriptor<TypeMetadata>> property = resolveAndValidate(propertyPath);

      if (property.isEmpty()) {
         throw log.getPredicatesOnEntityAliasNotAllowedException(propertyPath.asStringPath());
      }

      ComparisonExpr.Type comparisonType = switch (ctx.op.getType()) {
         case IckleLexer.EQUALS, IckleLexer.NOT_EQUAL -> ComparisonExpr.Type.EQUAL;
         case IckleLexer.GREATER -> ComparisonExpr.Type.GREATER;
         case IckleLexer.GREATER_EQUAL -> ComparisonExpr.Type.GREATER_OR_EQUAL;
         case IckleLexer.LESS -> ComparisonExpr.Type.LESS;
         case IckleLexer.LESS_EQUAL -> ComparisonExpr.Type.LESS_OR_EQUAL;
         default -> throw new IllegalArgumentException("Unknown comparison operator: " + ctx.op.getText());
      };

      // Push NOT BEFORE adding comparison if it's !=
      boolean isNotEqual = (ctx.op.getType() == IckleLexer.NOT_EQUAL);
      if (isNotEqual) {
         expressionBuilder.pushNot();
      }

      // Set currentPropertyPath temporarily for parameterValue
      currentPropertyPath = propertyPath;
      Object comparisonValue = parameterValue(value);

      String rootAlias = propertyPath.getAlias();
      checkAnalyzed(property, false);
      expressionBuilder.addComparison(property, comparisonType, comparisonValue, rootAlias);
      // Pop NOT AFTER adding comparison
      if (isNotEqual) {
         expressionBuilder.popBoolean();
      }

      return null;
   }

   @Override
   public Object visitEqualityExpression(IckleParser.EqualityExpressionContext ctx) {
      if (ctx.ftOccurrence() != null && ctx.fullTextExpression() != null) {
         String occur = ctx.ftOccurrence().getText();
         VirtualExpressionBuilder.Occur occurType = switch (occur) {
            case "+" -> VirtualExpressionBuilder.Occur.MUST;
            case "-" -> VirtualExpressionBuilder.Occur.MUST_NOT;
            case "!" -> VirtualExpressionBuilder.Occur.MUST_NOT;
            case "#" -> VirtualExpressionBuilder.Occur.FILTER;
            default -> VirtualExpressionBuilder.Occur.SHOULD;
         };

         expressionBuilder.pushFullTextOccur(occurType);
         visit(ctx.fullTextExpression());
         expressionBuilder.popFullTextOccur();
         return null;
      }

      return visitChildren(ctx);
   }

   @Override
   public Object visitLogicalAndExpression(IckleParser.LogicalAndExpressionContext ctx) {
      if (ctx.negatedExpression().size() > 1) {
         expressionBuilder.pushAnd();
      }

      visitChildren(ctx);

      if (ctx.negatedExpression().size() > 1) {
         expressionBuilder.popBoolean();
      }

      return null;
   }

   @Override
   public Object visitLogicalOrExpression(IckleParser.LogicalOrExpressionContext ctx) {
      if (ctx.logicalAndExpression().size() > 1) {
         expressionBuilder.pushOr();
      }

      visitChildren(ctx);

      if (ctx.logicalAndExpression().size() > 1) {
         expressionBuilder.popBoolean();
      }

      return null;
   }

   @Override
   public Object visitNegatedExpression(IckleParser.NegatedExpressionContext ctx) {
      if (ctx.not_key() != null) {
         expressionBuilder.pushNot();
      }

      visitChildren(ctx);

      if (ctx.not_key() != null) {
         expressionBuilder.popBoolean();
      }

      return null;
   }

   @Override
   public Object visitRelationalTail(IckleParser.RelationalTailContext ctx) {
      if (phase != VirtualExpressionBuilder.Phase.WHERE && phase != VirtualExpressionBuilder.Phase.HAVING) {
         return visitChildren(ctx);
      }

      // Handle comparison operators (<, >, <=, >=)
      if (ctx.op != null) {
         IckleParser.RelationalExpressionContext parent = (IckleParser.RelationalExpressionContext) ctx.getParent();

         PropertyPath<TypeDescriptor<TypeMetadata>> property;
         PropertyPath<TypeDescriptor<TypeMetadata>> pathForParam;

         // In HAVING, use the aggregation path if available
         if (phase == VirtualExpressionBuilder.Phase.HAVING && currentAggregationPath != null) {
            property = currentAggregationPath;
            pathForParam = property;
            currentAggregationPath = null;
         } else {
            if (containsAggregation(parent.additiveExpression())) {
               throw log.getFiltersCannotUseGroupingOrAggregationException();
            }
            String propertyPathText = parent.additiveExpression().getText();
            PropertyPath<TypeDescriptor<TypeMetadata>> propertyPath = buildPropertyPath(propertyPathText);
            property = resolveAlias(propertyPath);
            pathForParam = propertyPath;

            if (property.isEmpty()) {
               throw log.getPredicatesOnEntityAliasNotAllowedException(propertyPath.asStringPath());
            }
         }

         ComparisonExpr.Type comparisonType = switch (ctx.op.getType()) {
            case IckleLexer.GREATER -> ComparisonExpr.Type.GREATER;
            case IckleLexer.GREATER_EQUAL -> ComparisonExpr.Type.GREATER_OR_EQUAL;
            case IckleLexer.LESS -> ComparisonExpr.Type.LESS;
            case IckleLexer.LESS_EQUAL -> ComparisonExpr.Type.LESS_OR_EQUAL;
            default -> throw new IllegalArgumentException("Unknown operator: " + ctx.op.getText());
         };

         String value = ctx.additiveExpression().getText();

         currentPropertyPath = pathForParam;

         Object comparisonValue = parameterValue(value);

         checkAnalyzed(property, false);

         expressionBuilder.addComparison(property, comparisonType, comparisonValue, pathForParam.getAlias());

         return null;
      }

      // Handle NOT operator
      if (ctx.not_key() != null) {
         expressionBuilder.pushNot();
      }

      visitChildren(ctx);

      if (ctx.not_key() != null) {
         expressionBuilder.popBoolean();
      }

      return null;
   }

   private boolean containsAggregation(ParseTree tree) {
      if (tree instanceof IckleParser.CountFunctionContext || tree instanceof IckleParser.SimpleAggFunctionContext) {
         return true;
      }
      for (int i = 0; i < tree.getChildCount(); i++) {
         if (containsAggregation(tree.getChild(i))) {
            return true;
         }
      }
      return false;
   }

   @Override
   public Object visitBooleanLiteral(IckleParser.BooleanLiteralContext ctx) {
      if (phase != VirtualExpressionBuilder.Phase.WHERE && phase != VirtualExpressionBuilder.Phase.HAVING) {
         return visitChildren(ctx);
      }

      if (ctx.TRUE() != null) {
         expressionBuilder.addConstantBoolean(true);
         return null;
      }
      if (ctx.FALSE() != null) {
         expressionBuilder.addConstantBoolean(false);
         return null;
      }

      return visitChildren(ctx);
   }

   @Override
   public Object visitIsClause(IckleParser.IsClauseContext ctx) {
      if (phase != VirtualExpressionBuilder.Phase.WHERE && phase != VirtualExpressionBuilder.Phase.HAVING) {
         return visitChildren(ctx);
      }

      // Get property path from parent
      IckleParser.EqualityTailContext parent = (IckleParser.EqualityTailContext) ctx.getParent();
      IckleParser.EqualityExpressionContext eqParent = (IckleParser.EqualityExpressionContext) parent.getParent();

      String propertyPathText = eqParent.relationalExpression().getText();
      PropertyPath<TypeDescriptor<TypeMetadata>> propertyPath = buildPropertyPath(propertyPathText);
      PropertyPath<TypeDescriptor<TypeMetadata>> property = resolveAndValidate(propertyPath);

      if (property.isEmpty()) {
         throw log.getPredicatesOnEntityAliasNotAllowedException(propertyPath.asStringPath());
      }

      checkAnalyzed(property, false);

      // Check if it's IS NOT NULL
      if (ctx.not_key() != null) {
         expressionBuilder.pushNot();
      }

      expressionBuilder.addIsNull(property);

      if (ctx.not_key() != null) {
         expressionBuilder.popBoolean();
      }

      return null;
   }

   @Override
   public Object visitRelationalOperator(IckleParser.RelationalOperatorContext ctx) {
      // Common: resolve property path from parent
      IckleParser.RelationalTailContext tail = (IckleParser.RelationalTailContext) ctx.getParent();
      IckleParser.RelationalExpressionContext relExpr = (IckleParser.RelationalExpressionContext) tail.getParent();
      String propertyPathText = relExpr.additiveExpression().getText();
      currentPropertyPath = buildPropertyPath(propertyPathText);

      PropertyPath<TypeDescriptor<TypeMetadata>> property = resolveAndValidate(currentPropertyPath);
      if (property.isEmpty()) {
         throw log.getPredicatesOnEntityAliasNotAllowedException(currentPropertyPath.asStringPath());
      }

      // Handle LIKE operator
      if (ctx.like_key() != null) {
         // Get property path from parent
         PropertyPath<TypeDescriptor<TypeMetadata>> propertyPath = buildPropertyPath(propertyPathText);

         if (property.isEmpty()) {
            throw log.getPredicatesOnEntityAliasNotAllowedException(propertyPath.asStringPath());
         }

         // Get pattern
         String pattern = ctx.additiveExpression().getText();

         // Remove quotes
         if (pattern.startsWith("'") && pattern.endsWith("'")) {
            pattern = pattern.substring(1, pattern.length() - 1);
            pattern = pattern.replace("''", "'");
         }

         // Get escape character if present
         Character escapeChar = null;
         if (ctx.likeEscape() != null) {
            String escape = ctx.likeEscape().getText();
            if (escape.length() > 0) {
               escapeChar = escape.charAt(0);
            }
         }

         checkAnalyzed(property, false);

         expressionBuilder.addLike(property, pattern, escapeChar);

         return null;
      }

      // Delegate to specific visitors (visitGeoCircle, visitInList, visitBetweenList etc.)
      return visitChildren(ctx);
   }

   @Override
   public Object visitBetweenList(IckleParser.BetweenListContext ctx) {
      PropertyPath<TypeDescriptor<TypeMetadata>> property = resolveAndValidate(currentPropertyPath);
      checkAnalyzed(property, false);

      Object lowerValue = parameterValue(ctx.lowerExpr.getText());
      Object upperValue = parameterValue(ctx.upperExpr.getText());

      expressionBuilder.addRange(property, lowerValue, upperValue);
      return null;
   }

   @Override
   public Object visitInList(IckleParser.InListContext ctx) {
      PropertyPath<TypeDescriptor<TypeMetadata>> property = resolveAndValidate(currentPropertyPath);
      checkAnalyzed(property, false);

      List<Object> values = new ArrayList<>();
      for (IckleParser.AdditiveExpressionContext expr : ctx.additiveExpression()) {
         values.add(parameterValue(expr.getText()));
      }

      expressionBuilder.addIn(property, values);
      return null;
   }

   @Override
   public Object visitGeoCircle(IckleParser.GeoCircleContext ctx) {
      PropertyPath<TypeDescriptor<TypeMetadata>> property = resolveAndValidate(currentPropertyPath);
      checkSpatial(property);

      Object latValue = parameterValue(Double.class, ctx.latAtom.getText());
      Object lonValue = parameterValue(Double.class, ctx.lonAtom.getText());

      IckleParser.DistanceValContext distVal = ctx.radiusAtom;
      Object radiusValue = parameterValue(Double.class, distVal.constant().getText());

      String unit = "m";
      if (distVal.unitVal() != null) {
         unit = distVal.unitVal().getText();
      }
      Object unitValue = parameterValue(String.class, unit);

      expressionBuilder.whereBuilder().addSpatialWithinCircle(property, latValue, lonValue, radiusValue, unitValue);
      return null;
   }

   @Override
   public Object visitGeoBoundingBox(IckleParser.GeoBoundingBoxContext ctx) {
      PropertyPath<TypeDescriptor<TypeMetadata>> property = resolveAndValidate(currentPropertyPath);
      checkSpatial(property);

      Object tlLat = parameterValue(Double.class, ctx.tlLatAtom.getText());
      Object tlLon = parameterValue(Double.class, ctx.tlLonAtom.getText());
      Object brLat = parameterValue(Double.class, ctx.brLatAtom.getText());
      Object brLon = parameterValue(Double.class, ctx.brLonAtom.getText());

      expressionBuilder.whereBuilder().addSpatialWithinBox(property, tlLat, tlLon, brLat, brLon);
      return null;
   }

   @Override
   public Object visitGeoPolygon(IckleParser.GeoPolygonContext ctx) {
      PropertyPath<TypeDescriptor<TypeMetadata>> property = resolveAndValidate(currentPropertyPath);
      checkSpatial(property);

      List<Object> vector = new ArrayList<>();
      for (IckleParser.GeoPolygonArgContext arg : ctx.geoPolygonArg()) {
         if (arg.parameterSpecification() != null) {
            vector.add(parameterStringValue(arg.parameterSpecification().getText()));
         } else {
            String pointStr = "(" + arg.atom(0).getText() + ", " + arg.atom(1).getText() + ")";
            vector.add(pointStr);
         }
      }

      expressionBuilder.whereBuilder().addSpatialWithinPolygon(property, vector);
      return null;
   }

   @Override
   public Object visitDistanceFunction(IckleParser.DistanceFunctionContext ctx) {
      String propertyPathText = ctx.propertyReference().getText();
      PropertyPath<TypeDescriptor<TypeMetadata>> path = buildPropertyPath(propertyPathText);
      PropertyPath<TypeDescriptor<TypeMetadata>> resolved = resolveAlias(path);

      List<Object> args = new ArrayList<>();
      args.add(toDouble(ctx.lat));
      args.add(toDouble(ctx.lon));

      if (ctx.distanceFunctionUnit() != null) {
         args.add(ctx.distanceFunctionUnit().unitVal().getText());
      }

      FunctionPropertyPath<TypeMetadata> functionPath = new FunctionPropertyPath<>(
            resolved.getNodes(), Function.DISTANCE, args);

      if (phase == VirtualExpressionBuilder.Phase.SELECT) {
         if (projections == null) {
            projections = new ArrayList<>();
         }
         projections.add(functionPath);
      } else if (phase == VirtualExpressionBuilder.Phase.ORDER_BY) {
         currentPropertyPath = functionPath;
      }

      return null;
   }

   private static double toDouble(IckleParser.AtomContext atomContext) {
      return Double.parseDouble(atomContext.getText());
   }

   private PropertyPath<TypeDescriptor<TypeMetadata>> buildPropertyPath(String pathText) {
      String[] parts = pathText.split("\\.");
      List<PropertyPath.PropertyReference<TypeDescriptor<TypeMetadata>>> nodes = new ArrayList<>();

      for (int i = 0; i < parts.length; i++) {
         String name = parts[i];
         boolean isAlias = (i == 0 && (aliasToEntityType.containsKey(name) || aliasToPropertyPath.containsKey(name)));

         nodes.add(new PropertyPath.PropertyReference<>(name, null, isAlias));
      }
      PropertyPath<TypeDescriptor<TypeMetadata>> result = new PropertyPath<>(nodes);

      return result;
   }

   @Override
   public Object visitFullTextExpression(IckleParser.FullTextExpressionContext ctx) {
      if (phase != VirtualExpressionBuilder.Phase.WHERE) {
         return visitChildren(ctx);
      }

      // Set current property path for ftTerm to use
      String propertyPathText = ctx.ftFieldPath().getText();
      currentPropertyPath = buildPropertyPath(propertyPathText);

      // Visit children (ftBoostedQuery) - they'll use currentPropertyPath
      visitChildren(ctx);

      currentPropertyPath = null;

      return null;
   }

   @Override
   public Object visitFtClause(IckleParser.FtClauseContext ctx) {
      // Handle occurrence modifier (+, -, #, or nothing)
      if (ctx.ftOccurrence() != null) {
         String occur = ctx.ftOccurrence().getText();
         VirtualExpressionBuilder.Occur occurType = switch (occur) {
            case "+" -> VirtualExpressionBuilder.Occur.MUST;
            case "-" -> VirtualExpressionBuilder.Occur.MUST_NOT;
            case "#" -> VirtualExpressionBuilder.Occur.FILTER;
            default -> VirtualExpressionBuilder.Occur.SHOULD;
         };

         expressionBuilder.pushFullTextOccur(occurType);
         visit(ctx.ftBoostedQuery());
         expressionBuilder.popFullTextOccur();
      } else {
         visit(ctx.ftBoostedQuery());
      }

      return null;
   }

   @Override
   public Object visitFtBoostedQuery(IckleParser.FtBoostedQueryContext ctx) {
      if (ctx.ftBoost() != null) {
         String boostText = ctx.ftBoost().val.getText();
         float boost = Float.parseFloat(boostText);

         expressionBuilder.pushFullTextBoost(boost);
         visit(ctx.ftTermOrQuery());
         expressionBuilder.popFullTextBoost();
      } else {
         visit(ctx.ftTermOrQuery());
      }

      return null;
   }

   @Override
   public Object visitFtTermOrQuery(IckleParser.FtTermOrQueryContext ctx) {
      if (ctx.ftTerm() != null) {
         // Simple term
         visit(ctx.ftTerm());
      } else if (ctx.ftRange() != null) {
         // Range query
         visit(ctx.ftRange());
      } else if (ctx.LPAREN() != null) {
         // Parenthesized expression with conjunctions and OR
         List<IckleParser.FtConjunctionContext> conjunctions = ctx.ftConjunction();

         if (conjunctions.size() > 1) {
            // Multiple conjunctions with OR between them
            expressionBuilder.pushOr();
            for (var conj : conjunctions) {
               visit(conj);
            }
            expressionBuilder.popBoolean();
         } else {
            // Single conjunction
            visit(conjunctions.get(0));
         }
      }

      return null;
   }

   @Override
   public Object visitFtConjunction(IckleParser.FtConjunctionContext ctx) {
      List<IckleParser.FtClauseContext> clauses = ctx.ftClause();

      if (clauses.size() > 1) {
         boolean hasExplicitAnd = !ctx.ftAnd().isEmpty();
         if (hasExplicitAnd) {
            // Explicit AND between clauses
            expressionBuilder.pushAnd();
            for (var clause : clauses) {
               visit(clause);
            }
            expressionBuilder.popBoolean();
         } else {
            // No explicit operator = OR implicit
            expressionBuilder.pushOr();
            for (var clause : clauses) {
               visit(clause);
            }
            expressionBuilder.popBoolean();
         }
      } else {
         visit(clauses.get(0));
      }

      return null;
   }

   @Override
   public Object visitFtTerm(IckleParser.FtTermContext ctx) {
      if (phase != VirtualExpressionBuilder.Phase.WHERE) {
         return null;
      }

      // Get the current property (set by visitFullTextExpression)
      if (currentPropertyPath == null) {
         throw log.getLeftSideMustBeAPropertyPath();
      }

      PropertyPath<TypeDescriptor<TypeMetadata>> property = resolveAndValidate(currentPropertyPath);

      if (property.isEmpty()) {
         throw log.getPredicatesOnEntityAliasNotAllowedException(currentPropertyPath.asStringPath());
      }

      checkAnalyzed(property, true);

      // Check if it's a regex or a term
      if (ctx.REGEXP_LITERAL() != null) {
         // REGEX
         String regexText = ctx.REGEXP_LITERAL().getText();

         // Remove / delimiters
         if (regexText.startsWith("/") && regexText.endsWith("/")) {
            regexText = regexText.substring(1, regexText.length() - 1);
         }

         expressionBuilder.addFullTextRegexp(property, regexText);
      } else {
         // TERM
         IckleParser.FtLiteralOrParameterContext litOrParam = ctx.ftLiteralOrParameter();
         Object termValue;

         if (litOrParam.parameterSpecification() != null) {
            // Named parameter like :description
            String paramText = litOrParam.parameterSpecification().getText();
            termValue = parameterStringValue(paramText);
         } else {
            // String literal
            String termText = litOrParam.getText();
            if (termText.startsWith("'") && termText.endsWith("'")) {
               termText = termText.substring(1, termText.length() - 1);
               termText = termText.replace("''", "'");
            }
            termValue = termText;
         }

         // Get fuzzy
         Integer fuzzy = null;
         if (ctx.ftFuzzySlop() != null) {
            String fuzzyText = ctx.ftFuzzySlop().getText();
            fuzzy = "~".equals(fuzzyText) ? 2 : Integer.parseInt(fuzzyText.substring(1));
         }

         expressionBuilder.addFullTextTerm(property, termValue, fuzzy);
      }

      return null;
   }

   @Override
   public Object visitFtRange(IckleParser.FtRangeContext ctx) {
      if (extracted()) return null;

      if (currentPropertyPath == null) {
         throw log.getLeftSideMustBeAPropertyPath();
      }

      PropertyPath<TypeDescriptor<TypeMetadata>> property = resolveAlias(currentPropertyPath);

      if (property.isEmpty()) {
         throw log.getPredicatesOnEntityAliasNotAllowedException(currentPropertyPath.asStringPath());
      }

      String lowerText = ctx.lower.getText();
      String upperText = ctx.upper.getText();

      Object lower = null;
      Object upper = null;

      if (!"*".equals(lowerText)) {
         currentPropertyPath = resolveAlias(currentPropertyPath);
         lower = parameterValue(lowerText);
      }

      if (!"*".equals(upperText)) {
         upper = parameterValue(upperText);
      }

      boolean includeLower = ctx.rb.LSQUARE() != null; // [ = inclusive, { = exclusive
      boolean includeUpper = ctx.re.RSQUARE() != null; // ] = inclusive, } = exclusive

      expressionBuilder.addFullTextRange(property, includeLower, lower, upper, includeUpper);

      return null;
   }

   @Override
   public Object visitKnnExpression(IckleParser.KnnExpressionContext ctx) {
      if (phase != VirtualExpressionBuilder.Phase.WHERE) {
         return visitChildren(ctx);
      }

      // Get the field path
      String propertyPathText = ctx.vectorFieldPath().getText();
      currentPropertyPath = buildPropertyPath(propertyPathText);
      PropertyPath<TypeDescriptor<TypeMetadata>> property = resolveAndValidate(currentPropertyPath);

      if (property.isEmpty()) {
         throw log.getPredicatesOnEntityAliasNotAllowedException(currentPropertyPath.asStringPath());
      }

      checkIsVector(property);
      Class<?> expectedType = propertyHelper.getIndexedPropertyType(
            targetEntityMetadata,
            property.getNodeNamesWithoutAlias().toArray(new String[0]));

      // Parse vector: [7,6,7] or [:a]
      IckleParser.KnnTermContext knnTerm = ctx.knnTerm();
      IckleParser.VectorSearchContext vectorSearchCtx = knnTerm.vectorSearch();
      List<IckleParser.ExpressionContext> expressions = vectorSearchCtx.expression();

      // Parse knn distance: ~7 or ~:k
      Object knn = null;
      if (knnTerm.knnDistance() != null && knnTerm.knnDistance().val != null) {
         String knnText = knnTerm.knnDistance().val.getText();
         if (knnText.startsWith(":")) {
            knn = parameterStringValue(knnText);
         } else {
            knn = knnText;
         }
      }

      // Single named parameter like [:a]
      if (expressions.size() == 1 && expressions.get(0).getText().startsWith(":")) {
         ConstantValueExpr.ParamPlaceholder vectorParam;
         try {
            vectorParam = (ConstantValueExpr.ParamPlaceholder) parameterStringValue(expressions.get(0).getText());
         } catch (Throwable throwable) {
            throw log.knnVectorParameterNotValid();
         }
         expressionBuilder.addKnnPredicate(property, expectedType, vectorParam, knn);
      } else {
         // Literal vector like [7,6,7] or [1.0,2.0,3.0]
         List<Object> vector = new ArrayList<>();
         for (IckleParser.ExpressionContext e : expressions) {
            String text = e.getText();
            if (text.startsWith(":")) {
               vector.add(parameterStringValue(text));
            } else {
               vector.add(text);  // Pass as String like the old code
            }
         }
         expressionBuilder.addKnnPredicate(property, expectedType, vector, knn);
      }

      // Handle filtering clause
      if (knnTerm.filteringClause() != null) {
         filtering = true;
         visit(knnTerm.filteringClause().logicalExpression());
         filtering = false;
      }

      return null;
   }

   @Override
   public Object visitScoreFunction(IckleParser.ScoreFunctionContext ctx) {
      if (phase != VirtualExpressionBuilder.Phase.SELECT) {
         return visitChildren(ctx);
      }

      if (projections == null) {
         projections = new ArrayList<>();
      }

      // score(alias) -> ScorePropertyPath
      projections.add(new ScorePropertyPath<>());
      return null;
   }

   @Override
   public Object visitQualifiedJoin(IckleParser.QualifiedJoinContext ctx) {
      String pathText = ctx.path().getText();
      PropertyPath<TypeDescriptor<TypeMetadata>> joinPath = buildPropertyPath(pathText);

      if (ctx.aliasClause() != null) {
         String alias = ctx.aliasClause().aliasDeclaration().getText();
         aliasToPropertyPath.put(alias, joinPath);
      }

      return null;
   }

   private boolean extracted() {
      if (phase != VirtualExpressionBuilder.Phase.WHERE) {
         return true;
      }
      return false;
   }

   private void addComparisonPredicate(String value, ComparisonExpr.Type comparisonType) {
      if (currentPropertyPath == null) {
         throw log.getLeftSideMustBeAPropertyPath();
      }
      PropertyPath<TypeDescriptor<TypeMetadata>> property = resolveAlias(currentPropertyPath);
      if (property.isEmpty()) {
         throw log.getPredicatesOnEntityAliasNotAllowedException(currentPropertyPath.asStringPath());
      }
      String rootAlias = currentPropertyPath.getAlias();
      Object comparisonValue = parameterValue(value);
      checkAnalyzed(property, false);
      expressionBuilder.addComparison(property, comparisonType, comparisonValue, rootAlias);
   }

   private String findAggregation(ParseTree tree) {
      if (tree instanceof IckleParser.SimpleAggFunctionContext) {
         AggregationFunction aggregationFunction = getAggregationFunction((IckleParser.SimpleAggFunctionContext) tree);
         if (aggregationFunction != null) {
            return aggregationFunction.name();
         }
         return null;
      }
      for (int i = 0; i < tree.getChildCount(); i++) {
         String agg = findAggregation(tree.getChild(i));
         if (agg != null) {
            return agg;
         }
      }
      return null;
   }

   private Object parameterValue(Class<?> type, String value) {
      if (value.startsWith(":")) {
         String paramName = value.substring(1).trim();
         ConstantValueExpr.ParamPlaceholder namedParam =
               (ConstantValueExpr.ParamPlaceholder) namedParameters.get(paramName);
         if (namedParam == null) {
            namedParam = new ConstantValueExpr.ParamPlaceholder(paramName);
            namedParameters.put(paramName, namedParam);
         }
         return namedParam;
      } else {
         return propertyHelper.convertToPropertyType(type, value);
      }
   }

   private Object parameterValue(String value) {
      // Delete quotes if present
      if (value.startsWith("'") && value.endsWith("'")) {
         value = value.substring(1, value.length() - 1);
         // Unescape doubled single quotes
         value = value.replace("''", "'");
      } else if (value.startsWith("\"") && value.endsWith("\"")) {
         value = value.substring(1, value.length() - 1);
         // Unescape doubled double quotes
         value = value.replace("\"\"", "\"");
      }
      if (value.startsWith(":")) {
         // it's a named parameter
         String paramName = value.substring(1).trim();  //todo [anistor] trim should not be required!
         ConstantValueExpr.ParamPlaceholder namedParam = (ConstantValueExpr.ParamPlaceholder) this.namedParameters.get(paramName);
         if (namedParam == null) {
            namedParam = new ConstantValueExpr.ParamPlaceholder(paramName);
            this.namedParameters.put(paramName, namedParam);
         }
         return namedParam;
      } else {
         // it's a literal value given in the query string
         List<String> path = currentPropertyPath.getNodeNamesWithoutAlias();
         // create the complete path in case it's a join
         PropertyPath<TypeDescriptor<TypeMetadata>> fullPath = currentPropertyPath;
         if (fullPath.isAlias()) {
            PropertyPath<TypeDescriptor<TypeMetadata>> resolved = aliasToPropertyPath.get(fullPath.getAlias());
            if (resolved != null) {
               path.addAll(0, resolved.getNodeNamesWithoutAlias());
            }
         }

         return propertyHelper.convertToPropertyType(targetEntityMetadata, path.toArray(new String[path.size()]), value);
      }
   }

   private void checkAnalyzed(PropertyPath<?> propertyPath, boolean expectAnalyzed) {
      if (!expectAnalyzed) {
         if (fieldIndexingMetadata.isAnalyzed(propertyPath.asArrayPath())) {
            throw log.getQueryOnAnalyzedPropertyNotSupportedException(targetTypeName, propertyPath.asStringPath());
         }
         return;
      }

      if (!fieldIndexingMetadata.isAnalyzed(propertyPath.asArrayPath()) &&
            !fieldIndexingMetadata.isNormalized(propertyPath.asArrayPath())) {
         throw log.getFullTextQueryOnNotAalyzedPropertyNotSupportedException(targetTypeName, propertyPath.asStringPath());
      }
   }

   private PropertyPath<TypeDescriptor<TypeMetadata>> resolveAlias(PropertyPath<TypeDescriptor<TypeMetadata>> path) {
      List<PropertyPath.PropertyReference<TypeDescriptor<TypeMetadata>>> resolved = resolveAliasPath(path);
      if (path instanceof AggregationPropertyPath) {
         return new AggregationPropertyPath<>(((AggregationPropertyPath<TypeMetadata>) path).getAggregationFunction(), resolved);
      }
      if (path instanceof FunctionPropertyPath) {
         return new FunctionPropertyPath<>(resolved, ((FunctionPropertyPath<TypeMetadata>) path).getFunction(),
               ((FunctionPropertyPath<TypeMetadata>) path).getArgs());
      }
      return new PropertyPath<>(resolved);
   }

   private List<PropertyPath.PropertyReference<TypeDescriptor<TypeMetadata>>> resolveAliasPath(PropertyPath<TypeDescriptor<TypeMetadata>> path) {
      if (path.isAlias()) {
         String alias = path.getAlias();
         if (aliasToEntityType.containsKey(alias)) {
            return path.getNodesWithoutAlias();
         } else if (aliasToPropertyPath.containsKey(alias)) {
            PropertyPath<TypeDescriptor<TypeMetadata>> propertyPath = aliasToPropertyPath.get(alias);
            List<PropertyPath.PropertyReference<TypeDescriptor<TypeMetadata>>> resolvedAlias = resolveAliasPath(propertyPath);
            for (PropertyPath.PropertyReference<TypeDescriptor<TypeMetadata>> node : path.getNodesWithoutAlias()) {
               resolvedAlias.add(new PropertyPath.PropertyReference<>(node.getPropertyName(), node.getTypeDescriptor(), true));
            }
            return resolvedAlias;
         } else {
            throw log.getUnknownAliasException(alias);
         }
      }

      // Not marked as alias - check if first segment looks like an unknown alias
      // (has dots and first part is not a valid property)
      if (path.getLength() > 1) {
         String firstName = path.getNodes().get(0).getPropertyName();
         if (!propertyHelper.hasProperty(targetEntityMetadata, new String[]{firstName})) {
            throw log.getUnknownAliasException(firstName);
         }
      }

      return path.getNodesWithoutAlias();
   }

   private PropertyPath<TypeDescriptor<TypeMetadata>> resolveAndValidate(PropertyPath<TypeDescriptor<TypeMetadata>> path) {
      PropertyPath<TypeDescriptor<TypeMetadata>> resolved = resolveAlias(path);
      if (!resolved.isEmpty()) {
         String[] pathArray = resolved.asArrayPath();
         if (!propertyHelper.hasProperty(targetEntityMetadata, pathArray)) {
            throw log.getNoSuchPropertyException(targetTypeName, path.asStringPathWithoutAlias());
         }
      }
      return resolved;
   }

   private void checkSpatial(PropertyPath<?> propertyPath) {
      if (!fieldIndexingMetadata.isSpatial(propertyPath.asArrayPath())) {
         throw log.spatialQueryOnNotIndexedPropertyNotSupportedException(targetTypeName, propertyPath.asStringPath());
      }
   }

   public void checkIsVector(PropertyPath<?> propertyPath) {
      if (!fieldIndexingMetadata.isVector(propertyPath.asArrayPath())) {
         throw log.knnPredicateOnNotVectorField(targetTypeName, propertyPath.asStringPath());
      }
   }

   private Object parameterStringValue(String value) {
      if (value.startsWith(":")) {
         // it's a named parameter
         String paramName = value.substring(1).trim();  //todo [anistor] trim should not be required!
         ConstantValueExpr.ParamPlaceholder namedParam = (ConstantValueExpr.ParamPlaceholder) namedParameters.get(paramName);
         if (namedParam == null) {
            namedParam = new ConstantValueExpr.ParamPlaceholder(paramName);
            namedParameters.put(paramName, namedParam);
         }
         return namedParam;
      } else {
         return value;
      }
   }

   @Override
   public VirtualExpressionBuilder.Phase getPhase() {
      return phase;
   }

   @Override
   public boolean isFiltering() {
      return filtering;
   }
}
