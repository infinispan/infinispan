package org.infinispan.query.dsl.embedded.impl;

import static org.infinispan.query.logging.Log.CONTAINER;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.infinispan.AdvancedCache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.objectfilter.Matcher;
import org.infinispan.objectfilter.SortField;
import org.infinispan.objectfilter.impl.RowMatcher;
import org.infinispan.objectfilter.impl.aggregation.FieldAccumulator;
import org.infinispan.objectfilter.impl.ql.AggregationFunction;
import org.infinispan.objectfilter.impl.ql.PropertyPath;
import org.infinispan.objectfilter.impl.syntax.AggregationExpr;
import org.infinispan.objectfilter.impl.syntax.AndExpr;
import org.infinispan.objectfilter.impl.syntax.BooleShannonExpansion;
import org.infinispan.objectfilter.impl.syntax.BooleanExpr;
import org.infinispan.objectfilter.impl.syntax.ComparisonExpr;
import org.infinispan.objectfilter.impl.syntax.ConstantBooleanExpr;
import org.infinispan.objectfilter.impl.syntax.ConstantValueExpr;
import org.infinispan.objectfilter.impl.syntax.ExprVisitor;
import org.infinispan.objectfilter.impl.syntax.FullTextVisitor;
import org.infinispan.objectfilter.impl.syntax.IndexedFieldProvider;
import org.infinispan.objectfilter.impl.syntax.IsNullExpr;
import org.infinispan.objectfilter.impl.syntax.LikeExpr;
import org.infinispan.objectfilter.impl.syntax.NotExpr;
import org.infinispan.objectfilter.impl.syntax.OrExpr;
import org.infinispan.objectfilter.impl.syntax.PropertyValueExpr;
import org.infinispan.objectfilter.impl.syntax.SyntaxTreePrinter;
import org.infinispan.objectfilter.impl.syntax.ValueExpr;
import org.infinispan.objectfilter.impl.syntax.parser.AggregationPropertyPath;
import org.infinispan.objectfilter.impl.syntax.parser.IckleParsingResult;
import org.infinispan.objectfilter.impl.syntax.parser.ObjectPropertyHelper;
import org.infinispan.objectfilter.impl.syntax.parser.RowPropertyHelper;
import org.infinispan.query.clustered.DistributedIndexedQueryImpl;
import org.infinispan.query.core.impl.AggregatingQuery;
import org.infinispan.query.core.impl.EmbeddedQuery;
import org.infinispan.query.core.impl.EmptyResultQuery;
import org.infinispan.query.core.impl.HybridQuery;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.impl.BaseQuery;
import org.infinispan.query.dsl.impl.QueryStringCreator;
import org.infinispan.query.impl.ComponentRegistryUtils;
import org.infinispan.query.impl.IndexedQuery;
import org.infinispan.query.impl.IndexedQueryImpl;
import org.infinispan.query.impl.QueryDefinition;
import org.infinispan.query.logging.Log;
import org.infinispan.search.mapper.mapping.SearchMapping;
import org.infinispan.util.function.SerializableFunction;
import org.infinispan.util.logging.LogFactory;

/**
 * Adds indexing capability to the light (index-less) QueryEngine from query-core module.
 *
 * @author anistor@redhat.com
 * @since 7.2
 */
public class QueryEngine<TypeMetadata> extends org.infinispan.query.core.impl.QueryEngine<TypeMetadata> {

   private static final Log log = LogFactory.getLog(QueryEngine.class, Log.class);

   private static final int MAX_EXPANSION_COFACTORS = 16;

   /**
    * Is the cache indexed?
    */
   protected final boolean isIndexed;

   /**
    * Optional, lazily. This is {@code null} if the cache is not actually indexed.
    */
   private SearchMapping searchMapping;

   private static final SerializableFunction<AdvancedCache<?, ?>, QueryEngine<?>> queryEngineProvider = c -> c.getComponentRegistry().getComponent(QueryEngine.class);

   private final boolean broadcastQuery;
   private final int defaultMaxResults;

   public QueryEngine(AdvancedCache<?, ?> cache, boolean isIndexed) {
      this(cache, isIndexed, ObjectReflectionMatcher.class);
   }

   protected QueryEngine(AdvancedCache<?, ?> cache, boolean isIndexed, Class<? extends Matcher> matcherImplClass) {
      super(cache, matcherImplClass);
      Configuration configuration = cache.getCacheConfiguration();
      CacheMode cacheMode = configuration.clustering().cacheMode();
      this.broadcastQuery = cacheMode.isClustered() && !cacheMode.isReplicated();
      this.isIndexed = isIndexed;
      this.defaultMaxResults = configuration.query().defaultMaxResults();
   }

   protected SearchMapping getSearchMapping() {
      if (searchMapping == null) {
         searchMapping = ComponentRegistryUtils.getSearchMapping(cache);
      }
      return searchMapping;
   }

   public Class<? extends Matcher> getMatcherClass() {
      return matcherImplClass;
   }

   ObjectPropertyHelper<TypeMetadata> getPropertyHelper() {
      return propertyHelper;
   }

   @Override
   protected BaseQuery<?> buildQueryWithAggregations(QueryFactory queryFactory, String queryString, Map<String, Object> namedParameters, long startOffset, int maxResults, IckleParsingResult<TypeMetadata> parsingResult, boolean local) {
      if (parsingResult.getProjectedPaths() == null) {
         throw CONTAINER.groupingAndAggregationQueriesMustUseProjections();
      }

      LinkedHashMap<PropertyPath, RowPropertyHelper.ColumnMetadata> columns = new LinkedHashMap<>();

      if (parsingResult.getGroupBy() != null) {
         for (PropertyPath<?> p : parsingResult.getGroupBy()) {
            if (p instanceof AggregationPropertyPath) {
               throw CONTAINER.cannotHaveAggregationsInGroupByClause();  // should not really be possible because this was validated during parsing
            }
            // Duplicates in 'group by' are accepted and silently discarded. This behaviour is similar to SQL.
            if (!columns.containsKey(p)) {
               if (propertyHelper.isRepeatedProperty(parsingResult.getTargetEntityMetadata(), p.asArrayPath())) {
                  // this constraint will be relaxed later: https://issues.jboss.org/browse/ISPN-6015
                  throw CONTAINER.multivaluedPropertyCannotBeUsedInGroupBy(p.toString());
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
               throw CONTAINER.expressionMustBePartOfAggregateFunctionOrShouldBeIncludedInGroupByClause(p.toString());
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
                  throw CONTAINER.expressionMustBePartOfAggregateFunctionOrShouldBeIncludedInGroupByClause(p.toString());
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
               throw CONTAINER.expressionMustBePartOfAggregateFunctionOrShouldBeIncludedInGroupByClause(propertyValueExpr.toQueryString());
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

   @Override
   protected BaseQuery<?> buildQueryNoAggregations(QueryFactory queryFactory, String queryString, Map<String, Object> namedParameters,
                                                   long startOffset, int maxResults, IckleParsingResult<TypeMetadata> parsingResult, boolean local) {
      if (parsingResult.hasGroupingOrAggregations()) {
         throw CONTAINER.queryMustNotUseGroupingOrAggregation(); // may happen only due to internal programming error
      }

      if (!isIndexed && parsingResult.getWhereClause() != null) {
         boolean isFullTextQuery = parsingResult.getWhereClause().acceptVisitor(FullTextVisitor.INSTANCE);
         if (isFullTextQuery) {
            throw new IllegalStateException("The cache must be indexed in order to use full-text queries.");
         }
      }

      if (parsingResult.getSortFields() != null) {
         for (SortField sortField : parsingResult.getSortFields()) {
            PropertyPath<?> p = sortField.getPath();
            if (propertyHelper.isRepeatedProperty(parsingResult.getTargetEntityMetadata(), p.asArrayPath())) {
               throw CONTAINER.multivaluedPropertyCannotBeUsedInOrderBy(p.toString());
            }
         }
      }

      if (parsingResult.getProjectedPaths() != null) {
         for (PropertyPath<?> p : parsingResult.getProjectedPaths()) {
            if (propertyHelper.isRepeatedProperty(parsingResult.getTargetEntityMetadata(), p.asArrayPath())) {
               throw CONTAINER.multivaluedPropertyCannotBeProjected(p.asStringPath());
            }
         }
      }

      BooleanExpr normalizedWhereClause = booleanFilterNormalizer.normalize(parsingResult.getWhereClause());
      if (normalizedWhereClause == ConstantBooleanExpr.FALSE) {
         // the query is a contradiction, there are no matches
         return new EmptyResultQuery<>(queryFactory, cache, queryString, parsingResult.getStatementType(), namedParameters, startOffset, maxResults, queryStatistics);
      }

      if (!isIndexed) {
         return new EmbeddedQuery<>(this, queryFactory, cache, queryString, parsingResult.getStatementType(),
               namedParameters, parsingResult.getProjections(), startOffset, maxResults, defaultMaxResults,
               queryStatistics, local);
      }

      IndexedFieldProvider.FieldIndexingMetadata fieldIndexingMetadata = propertyHelper.getIndexedFieldProvider().get(parsingResult.getTargetEntityMetadata());

      boolean allProjectionsAreStored = true;
      LinkedHashMap<PropertyPath, List<Integer>> projectionsMap = null;
      if (parsingResult.getProjectedPaths() != null) {
         projectionsMap = new LinkedHashMap<>();
         for (int i = 0; i < parsingResult.getProjectedPaths().length; i++) {
            PropertyPath<?> p = parsingResult.getProjectedPaths()[i];
            List<Integer> idx = projectionsMap.get(p);
            if (idx == null) {
               idx = new ArrayList<>();
               projectionsMap.put(p, idx);
               if (!fieldIndexingMetadata.isProjectable(p.asArrayPath())) {
                  allProjectionsAreStored = false;
               }
            }
            idx.add(i);
         }
      }

      boolean allSortFieldsAreStored = true;
      SortField[] sortFields = parsingResult.getSortFields();
      if (sortFields != null) {
         // deduplicate sort fields
         LinkedHashMap<String, SortField> sortFieldMap = new LinkedHashMap<>();
         for (SortField sf : sortFields) {
            PropertyPath<?> p = sf.getPath();
            String asStringPath = p.asStringPath();
            if (!sortFieldMap.containsKey(asStringPath)) {
               sortFieldMap.put(asStringPath, sf);
               if (!fieldIndexingMetadata.isSortable(p.asArrayPath())) {
                  allSortFieldsAreStored = false;
               }
            }
         }
         sortFields = sortFieldMap.values().toArray(new SortField[sortFieldMap.size()]);
      }

      //todo [anistor] do not allow hybrid queries with fulltext. exception, allow a fully indexed query followed by in-memory aggregation. the aggregated or 'having' field should not be analyzed

      //todo [anistor] do we allow aggregation in fulltext queries?

      //todo [anistor] do not allow hybrid fulltext queries. all 'where' fields must be indexed. all projections must be stored.

      BooleShannonExpansion bse = new BooleShannonExpansion(MAX_EXPANSION_COFACTORS, fieldIndexingMetadata);
      BooleanExpr expansion = bse.expand(normalizedWhereClause);

      if (expansion == normalizedWhereClause) {  // identity comparison is intended here!
         // all involved fields are indexed, so go the Lucene way
         if (allSortFieldsAreStored) {
            if (allProjectionsAreStored) {
               // all projections are stored, so we can execute the query entirely against the index, and we can also sort using the index
               RowProcessor rowProcessor = null;
               if (parsingResult.getProjectedPaths() != null) {
                  if (projectionsMap.size() != parsingResult.getProjectedPaths().length) {
                     // but some projections are duplicated and Hibernate Serach does not allow duplicate projections ...
                     final Class<?>[] projectedTypes = new Class<?>[projectionsMap.size()];
                     final Object[] deduplicatedProjectedNullMarkers = parsingResult.getProjectedNullMarkers() != null ? new Object[projectedTypes.length] : null;
                     final int[] map = new int[parsingResult.getProjectedPaths().length];
                     int j = 0;
                     for (List<Integer> idx : projectionsMap.values()) {
                        int i = idx.get(0);
                        projectedTypes[j] = parsingResult.getProjectedTypes()[i];
                        if (deduplicatedProjectedNullMarkers != null) {
                           deduplicatedProjectedNullMarkers[j] = parsingResult.getProjectedNullMarkers()[i];
                        }
                        for (int k : idx) {
                           map[k] = j;
                        }
                        j++;
                     }

                     RowProcessor projectionProcessor = makeProjectionProcessor(projectedTypes, deduplicatedProjectedNullMarkers);
                     rowProcessor = inRow -> {
                        if (projectionProcessor != null) {
                           inRow = projectionProcessor.apply(inRow);
                        }
                        Object[] outRow = new Object[map.length];
                        for (int i = 0; i < map.length; i++) {
                           outRow[i] = inRow[map[i]];
                        }
                        return outRow;
                     };
                     PropertyPath[] deduplicatedProjection = projectionsMap.keySet().toArray(new PropertyPath[projectionsMap.size()]);
                     IckleParsingResult<TypeMetadata> fpr = makeFilterParsingResult(parsingResult, normalizedWhereClause, deduplicatedProjection, projectedTypes, deduplicatedProjectedNullMarkers, sortFields);
                     return new EmbeddedLuceneQuery<>(this, queryFactory, namedParameters, fpr, parsingResult.getProjections(), rowProcessor, startOffset, maxResults, local);
                  } else {
                     // happy case: no projections are duplicated
                     rowProcessor = makeProjectionProcessor(parsingResult.getProjectedTypes(), parsingResult.getProjectedNullMarkers());
                  }
               }
               return new EmbeddedLuceneQuery<>(this, queryFactory, namedParameters, parsingResult, parsingResult.getProjections(), rowProcessor, startOffset, maxResults, local);
            } else {
               IckleParsingResult<TypeMetadata> fpr = makeFilterParsingResult(parsingResult, normalizedWhereClause, null, null, null, sortFields);
               Query<?> indexQuery = new EmbeddedLuceneQuery<>(this, queryFactory, namedParameters, fpr, null, null, startOffset, maxResults, local);
               String projectionQueryStr = SyntaxTreePrinter.printTree(parsingResult.getTargetEntityName(), parsingResult.getProjectedPaths(), null, null);
               return new HybridQuery<>(queryFactory, cache, projectionQueryStr, parsingResult.getStatementType(), null, getObjectFilter(matcher, projectionQueryStr, null, null), startOffset, maxResults, indexQuery, queryStatistics, local);
            }
         } else {
            // projections may be stored but some sort fields are not so we need to query the index and then execute in-memory sorting and projecting in a second phase
            IckleParsingResult<TypeMetadata> fpr = makeFilterParsingResult(parsingResult, normalizedWhereClause, null, null, null, null);
            Query<?> indexQuery = new EmbeddedLuceneQuery<>(this, queryFactory, namedParameters, fpr, null, null, -1, -1, local);
            String projectionQueryStr = SyntaxTreePrinter.printTree(parsingResult.getTargetEntityName(), parsingResult.getProjectedPaths(), null, sortFields);
            return new HybridQuery<>(queryFactory, cache, projectionQueryStr, parsingResult.getStatementType(), null, getObjectFilter(matcher, projectionQueryStr, null, null), startOffset, maxResults, indexQuery, queryStatistics, local);
         }
      }

      if (expansion == ConstantBooleanExpr.TRUE) {
         // expansion leads to a full non-indexed query or the expansion is too long/complex
         return new EmbeddedQuery<>(this, queryFactory, cache, queryString, parsingResult.getStatementType(),
               namedParameters, parsingResult.getProjections(), startOffset, maxResults, defaultMaxResults,
               queryStatistics, local);
      }

      // some fields are indexed, run a hybrid query
      IckleParsingResult<TypeMetadata> fpr = makeFilterParsingResult(parsingResult, expansion, null, null, null, null);
      Query<?> expandedQuery = new EmbeddedLuceneQuery<>(this, queryFactory, namedParameters, fpr, null, null, -1, -1, local);
      return new HybridQuery<>(queryFactory, cache, queryString, parsingResult.getStatementType(), namedParameters, getObjectFilter(matcher, queryString, namedParameters, null), startOffset, maxResults, expandedQuery, queryStatistics, local);
   }

   /**
    * Make a new FilterParsingResult after normalizing the query. This FilterParsingResult is not supposed to have
    * grouping/aggregation.
    */
   private IckleParsingResult<TypeMetadata> makeFilterParsingResult(IckleParsingResult<TypeMetadata> parsingResult, BooleanExpr normalizedWhereClause,
                                                                    PropertyPath[] projection, Class<?>[] projectedTypes, Object[] projectedNullMarkers,
                                                                    SortField[] sortFields) {
      String queryString = SyntaxTreePrinter.printTree(parsingResult.getTargetEntityName(), projection, normalizedWhereClause, sortFields);
      return new IckleParsingResult<>(queryString, parsingResult.getStatementType(), parsingResult.getParameterNames(),
            normalizedWhereClause, null,
            parsingResult.getTargetEntityName(), parsingResult.getTargetEntityMetadata(),
            projection, projectedTypes, projectedNullMarkers, null, sortFields);
   }

   /**
    * Apply some post-processing to the result when we have projections.
    */
   protected RowProcessor makeProjectionProcessor(Class<?>[] projectedTypes, Object[] projectedNullMarkers) {
      // In embedded mode Hibernate Search is a real blessing as it does all the work for us already. Nothing to be done here.
      return null;
   }

   public SearchQueryBuilder buildSearchQuery(String queryString, Map<String, Object> namedParameters) {
      if (log.isDebugEnabled()) {
         log.debugf("Building Lucene query for Ickle query: %s", queryString);
      }

      if (!isIndexed) {
         throw CONTAINER.cannotRunLuceneQueriesIfNotIndexed(cache.getName());
      }

      IckleParsingResult<TypeMetadata> parsingResult = parse(queryString);
      if (parsingResult.hasGroupingOrAggregations()) {
         throw CONTAINER.groupAggregationsNotSupported();
      }

      return transformParsingResult(parsingResult, namedParameters);
   }

   public <E> IndexedQuery<E> buildLuceneQuery(IckleParsingResult<TypeMetadata> parsingResult,
                                               Map<String, Object> namedParameters, long startOffset, int maxResults, boolean local) {
      if (log.isDebugEnabled()) {
         log.debugf("Building Lucene query for Ickle query: %s", parsingResult.getQueryString());
      }

      if (!isIndexed) {
         throw CONTAINER.cannotRunLuceneQueriesIfNotIndexed(cache.getName());
      }

      SearchQueryBuilder searchQuery = transformParsingResult(parsingResult, namedParameters);

      IndexedQuery<?> cacheQuery = makeCacheQuery(parsingResult, searchQuery, namedParameters, local);
      if (startOffset >= 0) {
         cacheQuery = cacheQuery.firstResult((int) startOffset);
      }
      if (maxResults >= 0) {
         cacheQuery = cacheQuery.maxResults(maxResults);
      }
      return (IndexedQuery<E>) cacheQuery;
   }

   public SearchQueryBuilder transformParsingResult(IckleParsingResult<TypeMetadata> parsingResult, Map<String, Object> namedParameters) {
      SearchQueryParsingResult searchParsingResult;
      if (queryCache != null && parsingResult.getParameterNames().isEmpty()) {
         searchParsingResult = queryCache.get(cache.getName(), parsingResult.getQueryString(), null, SearchQueryParsingResult.class,
               (q, a) -> transformToSearchQueryParsingResult(parsingResult, namedParameters));
      } else {
         searchParsingResult = transformToSearchQueryParsingResult(parsingResult, namedParameters);
      }

      return searchParsingResult.builder(getSearchMapping().getMappingSession());
   }

   private SearchQueryParsingResult transformToSearchQueryParsingResult(IckleParsingResult<TypeMetadata> parsingResult, Map<String, Object> namedParameters) {
      SearchQueryMaker<TypeMetadata> queryMaker = new SearchQueryMaker<>(getSearchMapping(), propertyHelper);
      return queryMaker.transform(parsingResult, namedParameters, getTargetedClass(parsingResult), getTargetedNamedType(parsingResult));
   }

   @Override
   public IckleParsingResult<TypeMetadata> parse(String queryString) {
      return super.parse(queryString);    // TODO [anistor]  public just for org/infinispan/query/dsl/embedded/impl/EmbeddedQueryEngineTest.java
   }

   protected Class<?> getTargetedClass(IckleParsingResult<?> parsingResult) {
      return (Class<?>) parsingResult.getTargetEntityMetadata();
   }

   protected String getTargetedNamedType(IckleParsingResult<?> parsingResult) {
      // used by RemoteQueryEngine
      return null;
   }

   protected IndexedQuery<?> makeCacheQuery(IckleParsingResult<TypeMetadata> ickleParsingResult,
                                            SearchQueryBuilder searchQuery, Map<String, Object> namedParameters, boolean local) {
      if (!isIndexed) {
         throw CONTAINER.cannotRunLuceneQueriesIfNotIndexed(cache.getName());
      }
      String queryString = ickleParsingResult.getQueryString();
      if (broadcastQuery && !local) {
         QueryDefinition queryDefinition = new QueryDefinition(queryString, ickleParsingResult.getStatementType(),
               getQueryEngineProvider(), defaultMaxResults);
         queryDefinition.setNamedParameters(namedParameters);
         return new DistributedIndexedQueryImpl<>(queryDefinition, cache, queryStatistics, defaultMaxResults);
      }
      return new IndexedQueryImpl<>(queryString, ickleParsingResult.getStatementType(), searchQuery, cache,
            queryStatistics, defaultMaxResults);
   }

   protected SerializableFunction<AdvancedCache<?, ?>, QueryEngine<?>> getQueryEngineProvider() {
      return queryEngineProvider;
   }

   /**
    * A result processor that processes projections (rows). The input row is never {@code null}.
    * <p>
    * Applies some data conversion to some elements of the row. The input row can be modified in-place or a new one (of
    * equal or different size) can be created and returned. Some of the possible processing are type conversions and the
    * processing of null markers.
    */
   @FunctionalInterface
   protected interface RowProcessor extends Function<Object[], Object[]> {
   }
}
