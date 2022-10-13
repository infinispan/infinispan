package org.infinispan.query.core.impl;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.infinispan.AdvancedCache;
import org.infinispan.objectfilter.Matcher;
import org.infinispan.objectfilter.ObjectFilter;
import org.infinispan.objectfilter.SortField;
import org.infinispan.objectfilter.impl.BaseMatcher;
import org.infinispan.objectfilter.impl.ReflectionMatcher;
import org.infinispan.objectfilter.impl.RowMatcher;
import org.infinispan.objectfilter.impl.aggregation.FieldAccumulator;
import org.infinispan.objectfilter.impl.ql.AggregationFunction;
import org.infinispan.objectfilter.impl.ql.PropertyPath;
import org.infinispan.objectfilter.impl.syntax.AggregationExpr;
import org.infinispan.objectfilter.impl.syntax.AndExpr;
import org.infinispan.objectfilter.impl.syntax.BooleanExpr;
import org.infinispan.objectfilter.impl.syntax.BooleanFilterNormalizer;
import org.infinispan.objectfilter.impl.syntax.ComparisonExpr;
import org.infinispan.objectfilter.impl.syntax.ConstantBooleanExpr;
import org.infinispan.objectfilter.impl.syntax.ConstantValueExpr;
import org.infinispan.objectfilter.impl.syntax.ExprVisitor;
import org.infinispan.objectfilter.impl.syntax.FullTextVisitor;
import org.infinispan.objectfilter.impl.syntax.IsNullExpr;
import org.infinispan.objectfilter.impl.syntax.LikeExpr;
import org.infinispan.objectfilter.impl.syntax.NotExpr;
import org.infinispan.objectfilter.impl.syntax.OrExpr;
import org.infinispan.objectfilter.impl.syntax.PropertyValueExpr;
import org.infinispan.objectfilter.impl.syntax.SyntaxTreePrinter;
import org.infinispan.objectfilter.impl.syntax.ValueExpr;
import org.infinispan.objectfilter.impl.syntax.parser.AggregationPropertyPath;
import org.infinispan.objectfilter.impl.syntax.parser.IckleParser;
import org.infinispan.objectfilter.impl.syntax.parser.IckleParsingResult;
import org.infinispan.objectfilter.impl.syntax.parser.ObjectPropertyHelper;
import org.infinispan.objectfilter.impl.syntax.parser.RowPropertyHelper;
import org.infinispan.query.core.impl.eventfilter.IckleFilterAndConverter;
import org.infinispan.query.core.stats.impl.LocalQueryStatistics;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.impl.BaseQuery;
import org.infinispan.query.dsl.impl.QueryStringCreator;
import org.infinispan.util.logging.LogFactory;

/**
 * Builds Query object implementations based on an Ickle query string.
 *
 * @param <TypeMetadata> the metadata of the indexed entities, either a java.lang.Class or an
 *                       org.infinispan.protostream.descriptors.Descriptor
 */
public class QueryEngine<TypeMetadata> {

   private static final Log log = LogFactory.getLog(QueryEngine.class, Log.class);

   protected final AdvancedCache<?, ?> cache;

   protected final Matcher matcher;

   protected final Class<? extends Matcher> matcherImplClass;

   protected final ObjectPropertyHelper<TypeMetadata> propertyHelper;

   /**
    * Optional cache for internal intermediate query objects, to save some query parsing.
    */
   protected final QueryCache queryCache;

   private final int defaultMaxResults;

   protected LocalQueryStatistics queryStatistics;

   protected static final BooleanFilterNormalizer booleanFilterNormalizer = new BooleanFilterNormalizer();

   protected QueryEngine(AdvancedCache<?, ?> cache, Class<? extends Matcher> matcherImplClass) {
      this.cache = cache;
      this.matcherImplClass = matcherImplClass;
      this.queryCache = SecurityActions.getGlobalComponentRegistry(cache).getComponent(QueryCache.class);
      this.queryStatistics = SecurityActions.getCacheComponentRegistry(cache).getComponent(LocalQueryStatistics.class);
      this.matcher = SecurityActions.getCacheComponentRegistry(cache).getComponent(matcherImplClass);
      this.defaultMaxResults = cache.getCacheConfiguration().query().defaultMaxResults();
      propertyHelper = ((BaseMatcher<TypeMetadata, ?, ?>) matcher).getPropertyHelper();
   }

   QueryEngine(AdvancedCache<?, ?> cache) {
      this(cache, ReflectionMatcher.class);
   }

   public Query<?> buildQuery(QueryFactory queryFactory, IckleParsingResult<TypeMetadata> parsingResult, Map<String, Object> namedParameters, long startOffset, int maxResults, boolean local) {
      if (log.isDebugEnabled()) {
         log.debugf("Building query '%s' with parameters %s", parsingResult.getQueryString(), namedParameters);
      }

      BaseQuery<?> query = parsingResult.hasGroupingOrAggregations() ?
            buildQueryWithAggregations(queryFactory, parsingResult.getQueryString(), namedParameters, startOffset, maxResults, parsingResult, local) :
            buildQueryNoAggregations(queryFactory, parsingResult.getQueryString(), namedParameters, startOffset, maxResults, parsingResult, local);
      query.validateNamedParameters();
      return query;
   }

   protected BaseQuery<?> buildQueryWithAggregations(QueryFactory queryFactory, String queryString, Map<String, Object> namedParameters, long startOffset, int maxResults, IckleParsingResult<TypeMetadata> parsingResult, boolean local) {
      if (parsingResult.getProjectedPaths() == null) {
         throw log.groupingAndAggregationQueriesMustUseProjections();
      }

      LinkedHashMap<PropertyPath, RowPropertyHelper.ColumnMetadata> columns = new LinkedHashMap<>();

      if (parsingResult.getGroupBy() != null) {
         for (PropertyPath<?> p : parsingResult.getGroupBy()) {
            if (p instanceof AggregationPropertyPath) {
               throw log.cannotHaveAggregationsInGroupByClause();  // should not really be possible because this was validated during parsing
            }
            // Duplicates in 'group by' are accepted and silently discarded. This behaviour is similar to SQL.
            if (!columns.containsKey(p)) {
               if (propertyHelper.isRepeatedProperty(parsingResult.getTargetEntityMetadata(), p.asArrayPath())) {
                  // this constraint will be relaxed later: https://issues.jboss.org/browse/ISPN-6015
                  throw log.multivaluedPropertyCannotBeUsedInGroupBy(p.toString());
               }
               Class<?> propertyType = propertyHelper.getPrimitivePropertyType(parsingResult.getTargetEntityMetadata(), p.asArrayPath());
               int idx = columns.size();
               columns.put(p, new RowPropertyHelper.ColumnMetadata(idx, "C" + idx, propertyType));
            }
         }
      }
      final int noOfGroupingColumns = columns.size();

      for (int i = 0; i < parsingResult.getProjectedPaths().length; i++) {
         PropertyPath<?> p = parsingResult.getProjectedPaths()[i];
         RowPropertyHelper.ColumnMetadata c = columns.get(p);
         if (!(p instanceof AggregationPropertyPath)) {
            // this must be an already processed 'group by' field, or else it's an invalid query
            if (c == null || c.getColumnIndex() >= noOfGroupingColumns) {
               throw log.expressionMustBePartOfAggregateFunctionOrShouldBeIncludedInGroupByClause(p.toString());
            }
         }
         if (c == null) {
            Class<?> propertyType = FieldAccumulator.getOutputType(((AggregationPropertyPath) p).getAggregationFunction(), parsingResult.getProjectedTypes()[i]);
            int idx = columns.size();
            columns.put(p, new RowPropertyHelper.ColumnMetadata(idx, "C" + idx, propertyType));
         }
      }
      if (parsingResult.getSortFields() != null) {
         for (SortField sortField : parsingResult.getSortFields()) {
            PropertyPath<?> p = sortField.getPath();
            RowPropertyHelper.ColumnMetadata c = columns.get(p);
            if (!(p instanceof AggregationPropertyPath)) {
               // this must be an already processed 'group by' field, or else it's an invalid query
               if (c == null || c.getColumnIndex() >= noOfGroupingColumns) {
                  throw log.expressionMustBePartOfAggregateFunctionOrShouldBeIncludedInGroupByClause(p.toString());
               }
            }
            if (c == null) {
               Class<?> propertyType = propertyHelper.getPrimitivePropertyType(parsingResult.getTargetEntityMetadata(), p.asArrayPath());
               int idx = columns.size();
               columns.put(p, new RowPropertyHelper.ColumnMetadata(idx, "C" + idx, propertyType));
            }
         }
      }

      String havingClause = null;
      if (parsingResult.getHavingClause() != null) {
         BooleanExpr normalizedHavingClause = booleanFilterNormalizer.normalize(parsingResult.getHavingClause());
         if (normalizedHavingClause == ConstantBooleanExpr.FALSE) {
            return new EmptyResultQuery<>(queryFactory, cache, queryString, parsingResult.getStatementType(), namedParameters, startOffset, maxResults, queryStatistics);
         }
         if (normalizedHavingClause != ConstantBooleanExpr.TRUE) {
            havingClause = SyntaxTreePrinter.printTree(swapVariables(normalizedHavingClause, parsingResult.getTargetEntityMetadata(),
                  columns, namedParameters, propertyHelper));
         }
      }

      // validity of query is established at this point, no more checks needed

      for (PropertyPath<?> p : columns.keySet()) {
         if (propertyHelper.isRepeatedProperty(parsingResult.getTargetEntityMetadata(), p.asArrayPath())) {
            return buildQueryWithRepeatedAggregations(queryFactory, queryString, namedParameters, startOffset, maxResults,
                  parsingResult, havingClause, columns, noOfGroupingColumns, local);
         }
      }

      LinkedHashMap<String, Integer> inColumns = new LinkedHashMap<>();
      List<FieldAccumulator> accumulators = new LinkedList<>();
      RowPropertyHelper.ColumnMetadata[] _columns = new RowPropertyHelper.ColumnMetadata[columns.size()];
      for (PropertyPath<?> p : columns.keySet()) {
         RowPropertyHelper.ColumnMetadata c = columns.get(p);
         _columns[c.getColumnIndex()] = c;
         String asStringPath = p.asStringPath();
         Integer inIdx = inColumns.get(asStringPath);
         if (inIdx == null) {
            inIdx = inColumns.size();
            inColumns.put(asStringPath, inIdx);
         }
         if (p instanceof AggregationPropertyPath) {
            FieldAccumulator acc = FieldAccumulator.makeAccumulator(((AggregationPropertyPath) p).getAggregationFunction(), inIdx, c.getColumnIndex(), c.getPropertyType());
            accumulators.add(acc);
         }
      }

      StringBuilder firstPhaseQuery = new StringBuilder();
      firstPhaseQuery.append("SELECT ");
      {
         boolean isFirst = true;
         for (String p : inColumns.keySet()) {
            if (isFirst) {
               isFirst = false;
            } else {
               firstPhaseQuery.append(", ");
            }
            firstPhaseQuery.append(QueryStringCreator.DEFAULT_ALIAS).append('.').append(p);
         }
      }
      firstPhaseQuery.append(" FROM ").append(parsingResult.getTargetEntityName()).append(' ').append(QueryStringCreator.DEFAULT_ALIAS);
      if (parsingResult.getWhereClause() != null) {
         // the WHERE clause should not touch aggregated fields
         BooleanExpr normalizedWhereClause = booleanFilterNormalizer.normalize(parsingResult.getWhereClause());
         if (normalizedWhereClause == ConstantBooleanExpr.FALSE) {
            return new EmptyResultQuery<>(queryFactory, cache, queryString, parsingResult.getStatementType(), namedParameters, startOffset, maxResults, queryStatistics);
         }
         if (normalizedWhereClause != ConstantBooleanExpr.TRUE) {
            firstPhaseQuery.append(' ').append(SyntaxTreePrinter.printTree(normalizedWhereClause));
         }
      }

      StringBuilder secondPhaseQuery = new StringBuilder();
      secondPhaseQuery.append("SELECT ");
      {
         for (int i = 0; i < parsingResult.getProjectedPaths().length; i++) {
            PropertyPath<?> p = parsingResult.getProjectedPaths()[i];
            RowPropertyHelper.ColumnMetadata c = columns.get(p);
            if (i != 0) {
               secondPhaseQuery.append(", ");
            }
            secondPhaseQuery.append(c.getColumnName());
         }
      }
      secondPhaseQuery.append(" FROM Row ");
      if (havingClause != null) {
         secondPhaseQuery.append(' ').append(havingClause);
      }
      if (parsingResult.getSortFields() != null) {
         secondPhaseQuery.append(" ORDER BY ");
         boolean isFirst = true;
         for (SortField sortField : parsingResult.getSortFields()) {
            if (isFirst) {
               isFirst = false;
            } else {
               secondPhaseQuery.append(", ");
            }
            RowPropertyHelper.ColumnMetadata c = columns.get(sortField.getPath());
            secondPhaseQuery.append(c.getColumnName()).append(' ').append(sortField.isAscending() ? "ASC" : "DESC");
         }
      }

      // first phase: gather rows matching the 'where' clause
      String firstPhaseQueryStr = firstPhaseQuery.toString();
      // TODO ISPN-13986 Improve the efficiency of aggregation deletege them to Search!
      BaseQuery<?> baseQuery = buildQueryNoAggregations(queryFactory, firstPhaseQueryStr, namedParameters, -1, Integer.MAX_VALUE, parse(firstPhaseQueryStr), local);

      // second phase: grouping, aggregation, 'having' clause filtering, sorting and pagination
      String secondPhaseQueryStr = secondPhaseQuery.toString();
      return new AggregatingQuery<>(queryFactory, cache, secondPhaseQueryStr, namedParameters,
            noOfGroupingColumns, accumulators, false,
            getObjectFilter(new RowMatcher(_columns), secondPhaseQueryStr, namedParameters, null),
            startOffset, maxResults, baseQuery, queryStatistics, local);
   }

   /**
    * Swaps all occurrences of PropertyPaths in given expression tree (the HAVING clause) with new PropertyPaths
    * according to the mapping found in {@code columns} map.
    */
   private BooleanExpr swapVariables(BooleanExpr expr, TypeMetadata targetEntityMetadata,
                                     LinkedHashMap<PropertyPath, RowPropertyHelper.ColumnMetadata> columns,
                                     Map<String, Object> namedParameters, ObjectPropertyHelper<TypeMetadata> propertyHelper) {
      class PropertyReplacer extends ExprVisitor {

         @Override
         public BooleanExpr visit(NotExpr notExpr) {
            return new NotExpr(notExpr.getChild().acceptVisitor(this));
         }

         @Override
         public BooleanExpr visit(OrExpr orExpr) {
            List<BooleanExpr> visitedChildren = new ArrayList<>();
            for (BooleanExpr c : orExpr.getChildren()) {
               visitedChildren.add(c.acceptVisitor(this));
            }
            return new OrExpr(visitedChildren);
         }

         @Override
         public BooleanExpr visit(AndExpr andExpr) {
            List<BooleanExpr> visitedChildren = new ArrayList<>();
            for (BooleanExpr c : andExpr.getChildren()) {
               visitedChildren.add(c.acceptVisitor(this));
            }
            return new AndExpr(visitedChildren);
         }

         @Override
         public BooleanExpr visit(ConstantBooleanExpr constantBooleanExpr) {
            return constantBooleanExpr;
         }

         @Override
         public BooleanExpr visit(IsNullExpr isNullExpr) {
            return new IsNullExpr(isNullExpr.getChild().acceptVisitor(this));
         }

         @Override
         public BooleanExpr visit(ComparisonExpr comparisonExpr) {
            return new ComparisonExpr(comparisonExpr.getLeftChild().acceptVisitor(this), comparisonExpr.getRightChild(), comparisonExpr.getComparisonType());
         }

         @Override
         public BooleanExpr visit(LikeExpr likeExpr) {
            return new LikeExpr(likeExpr.getChild().acceptVisitor(this), likeExpr.getPattern(namedParameters));
         }

         @Override
         public ValueExpr visit(ConstantValueExpr constantValueExpr) {
            return constantValueExpr;
         }

         @Override
         public ValueExpr visit(PropertyValueExpr propertyValueExpr) {
            RowPropertyHelper.ColumnMetadata c = columns.get(propertyValueExpr.getPropertyPath());
            if (c == null) {
               throw log.expressionMustBePartOfAggregateFunctionOrShouldBeIncludedInGroupByClause(propertyValueExpr.toQueryString());
            }
            return new PropertyValueExpr(c.getColumnName(), propertyValueExpr.isRepeated(), propertyValueExpr.getPrimitiveType());
         }

         @Override
         public ValueExpr visit(AggregationExpr aggregationExpr) {
            RowPropertyHelper.ColumnMetadata c = columns.get(aggregationExpr.getPropertyPath());
            if (c == null) {
               Class<?> propertyType = propertyHelper.getPrimitivePropertyType(targetEntityMetadata, aggregationExpr.getPropertyPath().asArrayPath());
               propertyType = FieldAccumulator.getOutputType(aggregationExpr.getAggregationType(), propertyType);
               int idx = columns.size();
               c = new RowPropertyHelper.ColumnMetadata(idx, "C" + idx, propertyType);
               columns.put(aggregationExpr.getPropertyPath(), c);
               return new PropertyValueExpr(c.getColumnName(), aggregationExpr.isRepeated(), propertyType);
            }
            return new PropertyValueExpr(c.getColumnName(), aggregationExpr.isRepeated(), aggregationExpr.getPrimitiveType());
         }
      }
      return expr.acceptVisitor(new PropertyReplacer());
   }

   private BaseQuery<?> buildQueryWithRepeatedAggregations(QueryFactory queryFactory, String queryString, Map<String, Object> namedParameters, long startOffset, int maxResults,
                                                           IckleParsingResult<TypeMetadata> parsingResult, String havingClause,
                                                           LinkedHashMap<PropertyPath, RowPropertyHelper.ColumnMetadata> columns, int noOfGroupingColumns, boolean local) {
      // these types of aggregations can only be computed in memory

      StringBuilder firstPhaseQuery = new StringBuilder();
      firstPhaseQuery.append("FROM ").append(parsingResult.getTargetEntityName()).append(' ').append(QueryStringCreator.DEFAULT_ALIAS);
      if (parsingResult.getWhereClause() != null) {
         // the WHERE clause should not touch aggregated fields
         BooleanExpr normalizedWhereClause = booleanFilterNormalizer.normalize(parsingResult.getWhereClause());
         if (normalizedWhereClause == ConstantBooleanExpr.FALSE) {
            return new EmptyResultQuery<>(queryFactory, cache, queryString, parsingResult.getStatementType(), namedParameters, startOffset, maxResults, queryStatistics);
         }
         if (normalizedWhereClause != ConstantBooleanExpr.TRUE) {
            firstPhaseQuery.append(' ').append(SyntaxTreePrinter.printTree(normalizedWhereClause));
         }
      }
      String firstPhaseQueryStr = firstPhaseQuery.toString();
      BaseQuery<?> baseQuery = buildQueryNoAggregations(queryFactory, firstPhaseQueryStr, namedParameters, -1, -1, parse(firstPhaseQueryStr), local);

      List<FieldAccumulator> secondPhaseAccumulators = new LinkedList<>();
      List<FieldAccumulator> thirdPhaseAccumulators = new LinkedList<>();
      RowPropertyHelper.ColumnMetadata[] _columns = new RowPropertyHelper.ColumnMetadata[columns.size()];
      StringBuilder secondPhaseQuery = new StringBuilder();
      secondPhaseQuery.append("SELECT ");
      for (PropertyPath<?> p : columns.keySet()) {
         RowPropertyHelper.ColumnMetadata c = columns.get(p);
         if (c.getColumnIndex() > 0) {
            secondPhaseQuery.append(", ");
         }
         // only multi-valued fields need to be accumulated in this phase; for the others the accumulator is null
         if (p instanceof AggregationPropertyPath) {
            FieldAccumulator acc = FieldAccumulator.makeAccumulator(((AggregationPropertyPath) p).getAggregationFunction(), c.getColumnIndex(), c.getColumnIndex(), c.getPropertyType());
            if (propertyHelper.isRepeatedProperty(parsingResult.getTargetEntityMetadata(), p.asArrayPath())) {
               secondPhaseAccumulators.add(acc);
               if (((AggregationPropertyPath) p).getAggregationFunction() == AggregationFunction.COUNT) {
                  c = new RowPropertyHelper.ColumnMetadata(c.getColumnIndex(), c.getColumnName(), Long.class);
                  acc = FieldAccumulator.makeAccumulator(AggregationFunction.SUM, c.getColumnIndex(), c.getColumnIndex(), Long.class);
               }
            } else {
               secondPhaseAccumulators.add(null);
            }
            thirdPhaseAccumulators.add(acc);
         } else {
            secondPhaseAccumulators.add(null);
         }
         secondPhaseQuery.append(QueryStringCreator.DEFAULT_ALIAS).append('.').append(p.asStringPath());
         _columns[c.getColumnIndex()] = c;
      }
      secondPhaseQuery.append(" FROM ").append(parsingResult.getTargetEntityName()).append(' ').append(QueryStringCreator.DEFAULT_ALIAS);
      String secondPhaseQueryStr = secondPhaseQuery.toString();

      HybridQuery<?, ?> projectingAggregatingQuery = new HybridQuery<>(queryFactory, cache,
            secondPhaseQueryStr, parsingResult.getStatementType(), namedParameters,
            getObjectFilter(matcher, secondPhaseQueryStr, namedParameters, secondPhaseAccumulators),
            startOffset, maxResults, baseQuery, queryStatistics, local);

      StringBuilder thirdPhaseQuery = new StringBuilder();
      thirdPhaseQuery.append("SELECT ");
      for (int i = 0; i < parsingResult.getProjectedPaths().length; i++) {
         PropertyPath<?> p = parsingResult.getProjectedPaths()[i];
         RowPropertyHelper.ColumnMetadata c = columns.get(p);
         if (i != 0) {
            thirdPhaseQuery.append(", ");
         }
         thirdPhaseQuery.append(c.getColumnName());
      }
      thirdPhaseQuery.append(" FROM Row ");
      if (havingClause != null) {
         thirdPhaseQuery.append(' ').append(havingClause);
      }
      if (parsingResult.getSortFields() != null) {
         thirdPhaseQuery.append(" ORDER BY ");
         boolean isFirst = true;
         for (SortField sortField : parsingResult.getSortFields()) {
            if (isFirst) {
               isFirst = false;
            } else {
               thirdPhaseQuery.append(", ");
            }
            RowPropertyHelper.ColumnMetadata c = columns.get(sortField.getPath());
            thirdPhaseQuery.append(c.getColumnName()).append(' ').append(sortField.isAscending() ? "ASC" : "DESC");
         }
      }

      String thirdPhaseQueryStr = thirdPhaseQuery.toString();
      return new AggregatingQuery<>(queryFactory, cache, thirdPhaseQueryStr, namedParameters,
            noOfGroupingColumns, thirdPhaseAccumulators, true,
            getObjectFilter(new RowMatcher(_columns), thirdPhaseQueryStr, namedParameters, null),
            startOffset, maxResults, projectingAggregatingQuery, queryStatistics, local);
   }

   protected BaseQuery<?> buildQueryNoAggregations(QueryFactory queryFactory, String queryString, Map<String, Object> namedParameters,
                                                   long startOffset, int maxResults, IckleParsingResult<TypeMetadata> parsingResult, boolean local) {
      if (parsingResult.hasGroupingOrAggregations()) {
         throw log.queryMustNotUseGroupingOrAggregation(); // may happen only due to internal programming error
      }

      if (parsingResult.getWhereClause() != null) {
         boolean isFullTextQuery = parsingResult.getWhereClause().acceptVisitor(FullTextVisitor.INSTANCE);
         if (isFullTextQuery) {
            throw new IllegalStateException("The cache must be indexed in order to use full-text queries.");
         }
      }

      if (parsingResult.getSortFields() != null) {
         for (SortField sortField : parsingResult.getSortFields()) {
            PropertyPath<?> p = sortField.getPath();
            if (propertyHelper.isRepeatedProperty(parsingResult.getTargetEntityMetadata(), p.asArrayPath())) {
               throw log.multivaluedPropertyCannotBeUsedInOrderBy(p.toString());
            }
         }
      }

      if (parsingResult.getProjectedPaths() != null) {
         for (PropertyPath<?> p : parsingResult.getProjectedPaths()) {
            if (propertyHelper.isRepeatedProperty(parsingResult.getTargetEntityMetadata(), p.asArrayPath())) {
               throw log.multivaluedPropertyCannotBeProjected(p.asStringPath());
            }
         }
      }

      BooleanExpr normalizedWhereClause = booleanFilterNormalizer.normalize(parsingResult.getWhereClause());
      if (normalizedWhereClause == ConstantBooleanExpr.FALSE) {
         // the query is a contradiction, there are no matches
         return new EmptyResultQuery<>(queryFactory, cache, queryString, parsingResult.getStatementType(), namedParameters, startOffset, maxResults, queryStatistics);
      }

      return new EmbeddedQuery<>(this, queryFactory, cache, queryString, parsingResult.getStatementType(), namedParameters,
            parsingResult.getProjections(), startOffset, maxResults, defaultMaxResults, queryStatistics, local);
   }

   protected IckleParsingResult<TypeMetadata> parse(String queryString) {
      return queryCache != null
            ? queryCache.get(cache.getName(), queryString, null, IckleParsingResult.class, (qs, accumulators) -> IckleParser.parse(qs, propertyHelper))
            : IckleParser.parse(queryString, propertyHelper);
   }

   protected final ObjectFilter getObjectFilter(Matcher matcher, String queryString, Map<String, Object> namedParameters, List<FieldAccumulator> accumulators) {
      ObjectFilter objectFilter = queryCache != null
            ? queryCache.get(cache.getName(), queryString, accumulators, matcher.getClass(), matcher::getObjectFilter)
            : matcher.getObjectFilter(queryString, accumulators);
      return namedParameters != null ? objectFilter.withParameters(namedParameters) : objectFilter;
   }

   protected final IckleFilterAndConverter createAndWireFilter(String queryString, Map<String, Object> namedParameters) {
      IckleFilterAndConverter filter = createFilter(queryString, namedParameters);
      SecurityActions.getCacheComponentRegistry(cache).wireDependencies(filter);
      return filter;
   }

   protected IckleFilterAndConverter createFilter(String queryString, Map<String, Object> namedParameters) {
      return new IckleFilterAndConverter(queryString, namedParameters, matcherImplClass);
   }
}
