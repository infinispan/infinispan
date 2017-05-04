package org.infinispan.query.dsl.embedded.impl;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.hibernate.search.spi.SearchIntegrator;
import org.infinispan.AdvancedCache;
import org.infinispan.objectfilter.Matcher;
import org.infinispan.objectfilter.ObjectFilter;
import org.infinispan.objectfilter.SortField;
import org.infinispan.objectfilter.impl.BaseMatcher;
import org.infinispan.objectfilter.impl.RowMatcher;
import org.infinispan.objectfilter.impl.aggregation.FieldAccumulator;
import org.infinispan.objectfilter.impl.ql.AggregationFunction;
import org.infinispan.objectfilter.impl.ql.PropertyPath;
import org.infinispan.objectfilter.impl.syntax.AggregationExpr;
import org.infinispan.objectfilter.impl.syntax.AndExpr;
import org.infinispan.objectfilter.impl.syntax.BooleShannonExpansion;
import org.infinispan.objectfilter.impl.syntax.BooleanExpr;
import org.infinispan.objectfilter.impl.syntax.BooleanFilterNormalizer;
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
import org.infinispan.objectfilter.impl.syntax.parser.IckleParser;
import org.infinispan.objectfilter.impl.syntax.parser.IckleParsingResult;
import org.infinispan.objectfilter.impl.syntax.parser.ObjectPropertyHelper;
import org.infinispan.objectfilter.impl.syntax.parser.RowPropertyHelper;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.impl.BaseQuery;
import org.infinispan.query.dsl.impl.QueryStringCreator;
import org.infinispan.query.impl.ComponentRegistryUtils;
import org.infinispan.query.logging.Log;
import org.infinispan.security.AuthorizationManager;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.util.logging.LogFactory;

/**
 * @author anistor@redhat.com
 * @since 7.2
 */
public class QueryEngine<TypeMetadata> {

   private static final Log log = LogFactory.getLog(QueryEngine.class, Log.class);

   private static final int MAX_EXPANSION_COFACTORS = 16;

   private final AuthorizationManager authorizationManager;

   protected final AdvancedCache<?, ?> cache;

   /**
    * Is the cache indexed?
    */
   protected final boolean isIndexed;

   protected final Matcher matcher;

   protected final Class<? extends Matcher> matcherImplClass;

   protected final ObjectPropertyHelper<TypeMetadata> propertyHelper;

   protected final LuceneQueryMaker.FieldBridgeAndAnalyzerProvider<TypeMetadata> fieldBridgeAndAnalyzerProvider;

   /**
    * Optional cache for query objects.
    */
   private final QueryCache queryCache;

   /**
    * Optional, lazily acquired. This is {@code null} if the cache is not indexed.
    */
   private SearchManager searchManager;

   /**
    * Optional, lazily acquired form the {@link SearchManager}. This is {@code null} if the cache is not indexed.
    */
   private SearchIntegrator searchFactory;

   private final BooleanFilterNormalizer booleanFilterNormalizer = new BooleanFilterNormalizer();

   protected QueryEngine(AdvancedCache<?, ?> cache, boolean isIndexed, Class<? extends Matcher> matcherImplClass, LuceneQueryMaker.FieldBridgeAndAnalyzerProvider<TypeMetadata> fieldBridgeAndAnalyzerProvider) {
      this.cache = cache;
      this.isIndexed = isIndexed;
      this.matcherImplClass = matcherImplClass;
      this.queryCache = ComponentRegistryUtils.getQueryCache(cache);
      this.authorizationManager = SecurityActions.getCacheAuthorizationManager(cache);
      this.matcher = SecurityActions.getCacheComponentRegistry(cache).getComponent(matcherImplClass);
      propertyHelper = ((BaseMatcher<TypeMetadata, ?, ?>) matcher).getPropertyHelper();
      if (fieldBridgeAndAnalyzerProvider == null && propertyHelper instanceof HibernateSearchPropertyHelper) {
         this.fieldBridgeAndAnalyzerProvider = (LuceneQueryMaker.FieldBridgeAndAnalyzerProvider<TypeMetadata>) (((HibernateSearchPropertyHelper) propertyHelper).getDefaultFieldBridgeProvider());
      } else {
         this.fieldBridgeAndAnalyzerProvider = fieldBridgeAndAnalyzerProvider;
      }
   }

   protected SearchManager getSearchManager() {
      if (!isIndexed) {
         throw new IllegalStateException("Cache is not indexed");
      }
      if (searchManager == null) {
         searchManager = Search.getSearchManager(cache);
      }
      return searchManager;
   }

   protected SearchIntegrator getSearchFactory() {
      if (searchFactory == null) {
         searchFactory = getSearchManager().unwrap(SearchIntegrator.class);
      }
      return searchFactory;
   }

   protected BaseQuery buildQuery(QueryFactory queryFactory, IckleParsingResult<TypeMetadata> parsingResult, Map<String, Object> namedParameters, long startOffset, int maxResults) {
      if (log.isDebugEnabled()) {
         log.debugf("Building query '%s' with parameters %s", parsingResult.getQueryString(), namedParameters);
      }
      if (authorizationManager != null) {
         authorizationManager.checkPermission(AuthorizationPermission.BULK_READ);
      }
      BaseQuery query = parsingResult.hasGroupingOrAggregations() ?
            buildQueryWithAggregations(queryFactory, parsingResult.getQueryString(), namedParameters, startOffset, maxResults, parsingResult) :
            buildQueryNoAggregations(queryFactory, parsingResult.getQueryString(), namedParameters, startOffset, maxResults, parsingResult);
      query.validateNamedParameters();
      return query;
   }

   private BaseQuery buildQueryWithAggregations(QueryFactory queryFactory, String queryString, Map<String, Object> namedParameters, long startOffset, int maxResults, IckleParsingResult<TypeMetadata> parsingResult) {
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
            return new EmptyResultQuery(queryFactory, cache, queryString, namedParameters, startOffset, maxResults);
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
                  parsingResult, havingClause, columns, noOfGroupingColumns);
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
            return new EmptyResultQuery(queryFactory, cache, queryString, namedParameters, startOffset, maxResults);
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
      BaseQuery baseQuery = buildQueryNoAggregations(queryFactory, firstPhaseQueryStr, namedParameters, -1, -1, parse(firstPhaseQueryStr));

      // second phase: grouping, aggregation, 'having' clause filtering, sorting and pagination
      String secondPhaseQueryStr = secondPhaseQuery.toString();
      return new AggregatingQuery(queryFactory, cache, secondPhaseQueryStr, namedParameters,
            noOfGroupingColumns, accumulators, false,
            getObjectFilter(new RowMatcher(_columns), secondPhaseQueryStr, namedParameters, null),
            startOffset, maxResults, baseQuery);
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

   private BaseQuery buildQueryWithRepeatedAggregations(QueryFactory queryFactory, String queryString, Map<String, Object> namedParameters, long startOffset, int maxResults,
                                                        IckleParsingResult<TypeMetadata> parsingResult, String havingClause,
                                                        LinkedHashMap<PropertyPath, RowPropertyHelper.ColumnMetadata> columns, int noOfGroupingColumns) {
      // these types of aggregations can only be computed in memory

      StringBuilder firstPhaseQuery = new StringBuilder();
      firstPhaseQuery.append("FROM ").append(parsingResult.getTargetEntityName()).append(' ').append(QueryStringCreator.DEFAULT_ALIAS);
      if (parsingResult.getWhereClause() != null) {
         // the WHERE clause should not touch aggregated fields
         BooleanExpr normalizedWhereClause = booleanFilterNormalizer.normalize(parsingResult.getWhereClause());
         if (normalizedWhereClause == ConstantBooleanExpr.FALSE) {
            return new EmptyResultQuery(queryFactory, cache, queryString, namedParameters, startOffset, maxResults);
         }
         if (normalizedWhereClause != ConstantBooleanExpr.TRUE) {
            firstPhaseQuery.append(' ').append(SyntaxTreePrinter.printTree(normalizedWhereClause));
         }
      }
      String firstPhaseQueryStr = firstPhaseQuery.toString();
      BaseQuery baseQuery = buildQueryNoAggregations(queryFactory, firstPhaseQueryStr, namedParameters, -1, -1, parse(firstPhaseQueryStr));

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

      HybridQuery projectingAggregatingQuery = new HybridQuery(queryFactory, cache,
            secondPhaseQueryStr, namedParameters,
            getObjectFilter(matcher, secondPhaseQueryStr, namedParameters, secondPhaseAccumulators),
            -1, -1, baseQuery);

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
      return new AggregatingQuery(queryFactory, cache, thirdPhaseQueryStr, namedParameters,
            noOfGroupingColumns, thirdPhaseAccumulators, true,
            getObjectFilter(new RowMatcher(_columns), thirdPhaseQueryStr, namedParameters, null),
            startOffset, maxResults, projectingAggregatingQuery);
   }

   private BaseQuery buildQueryNoAggregations(QueryFactory queryFactory, String queryString, Map<String, Object> namedParameters,
                                              long startOffset, int maxResults, IckleParsingResult<TypeMetadata> parsingResult) {
      if (parsingResult.hasGroupingOrAggregations()) {
         throw log.queryMustNotUseGroupingOrAggregation(); // may happen only due to internal programming error
      }

      boolean isFullTextQuery;
      if (parsingResult.getWhereClause() != null) {
         isFullTextQuery = parsingResult.getWhereClause().acceptVisitor(FullTextVisitor.INSTANCE);
         if (!isIndexed && isFullTextQuery) {
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
         return new EmptyResultQuery(queryFactory, cache, queryString, namedParameters, startOffset, maxResults);
      }

      // if cache is indexed but there is no actual 'where' filter clause and we do have sorting or projections we should still use the index, otherwise just go for a non-indexed fetch-all
      if (!isIndexed || (normalizedWhereClause == null || normalizedWhereClause == ConstantBooleanExpr.TRUE) && parsingResult.getProjections() == null && parsingResult.getSortFields() == null) {
         // fully non-indexed execution because the filter matches everything or there is no indexing at all
         return new EmbeddedQuery(this, queryFactory, cache, queryString, namedParameters, parsingResult.getProjections(), startOffset, maxResults);
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
               if (!fieldIndexingMetadata.isStored(p.asArrayPath())) {
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
               if (!fieldIndexingMetadata.isStored(p.asArrayPath())) {
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
                     // but some projections are duplicated ...
                     final Class<?>[] projectedTypes = new Class<?>[projectionsMap.size()];
                     final int[] map = new int[parsingResult.getProjectedPaths().length];
                     int j = 0;
                     for (List<Integer> idx : projectionsMap.values()) {
                        int i = idx.get(0);
                        projectedTypes[j] = parsingResult.getProjectedTypes()[i];
                        for (int k : idx) {
                           map[k] = j;
                        }
                        j++;
                     }

                     RowProcessor projectionProcessor = makeProjectionProcessor(projectedTypes);
                     rowProcessor = inRow -> {
                        if (projectionProcessor != null) {
                           inRow = projectionProcessor.process(inRow);
                        }
                        Object[] outRow = new Object[map.length];
                        for (int i = 0; i < map.length; i++) {
                           outRow[i] = inRow[map[i]];
                        }
                        return outRow;
                     };
                     PropertyPath[] deduplicatedProjection = projectionsMap.keySet().toArray(new PropertyPath[projectionsMap.size()]);
                     IckleParsingResult<TypeMetadata> fpr = makeFilterParsingResult(parsingResult, normalizedWhereClause, deduplicatedProjection, projectedTypes, sortFields);
                     return new EmbeddedLuceneQuery<>(this, queryFactory, namedParameters, fpr, parsingResult.getProjections(), makeResultProcessor(rowProcessor), startOffset, maxResults);
                  } else {
                     rowProcessor = makeProjectionProcessor(parsingResult.getProjectedTypes());
                  }
               }
               return new EmbeddedLuceneQuery<>(this, queryFactory, namedParameters, parsingResult, parsingResult.getProjections(), makeResultProcessor(rowProcessor), startOffset, maxResults);
            } else {
               IckleParsingResult<TypeMetadata> fpr = makeFilterParsingResult(parsingResult, normalizedWhereClause, null, null, sortFields);
               Query indexQuery = new EmbeddedLuceneQuery<>(this, queryFactory, namedParameters, fpr, null, makeResultProcessor(null), startOffset, maxResults);
               String projectionQueryStr = SyntaxTreePrinter.printTree(parsingResult.getTargetEntityName(), parsingResult.getProjectedPaths(), null, null);
               return new HybridQuery(queryFactory, cache, projectionQueryStr, null, getObjectFilter(matcher, projectionQueryStr, null, null), -1, -1, indexQuery);
            }
         } else {
            // projections may be stored but some sort fields are not so we need to query the index and then execute in-memory sorting and projecting in a second phase
            IckleParsingResult<TypeMetadata> fpr = makeFilterParsingResult(parsingResult, normalizedWhereClause, null, null, null);
            Query indexQuery = new EmbeddedLuceneQuery<>(this, queryFactory, namedParameters, fpr, null, makeResultProcessor(null), -1, -1);
            String projectionQueryStr = SyntaxTreePrinter.printTree(parsingResult.getTargetEntityName(), parsingResult.getProjectedPaths(), null, sortFields);
            return new HybridQuery(queryFactory, cache, projectionQueryStr, null, getObjectFilter(matcher, projectionQueryStr, null, null), startOffset, maxResults, indexQuery);
         }
      }

      if (expansion == ConstantBooleanExpr.TRUE) {
         // expansion leads to a full non-indexed query or the expansion is too long/complex
         return new EmbeddedQuery(this, queryFactory, cache, queryString, namedParameters, parsingResult.getProjections(), startOffset, maxResults);
      }

      // some fields are indexed, run a hybrid query
      IckleParsingResult<TypeMetadata> fpr = makeFilterParsingResult(parsingResult, expansion, null, null, null);
      Query expandedQuery = new EmbeddedLuceneQuery<>(this, queryFactory, namedParameters, fpr, null, makeResultProcessor(null), -1, -1);
      return new HybridQuery(queryFactory, cache, queryString, namedParameters, getObjectFilter(matcher, queryString, namedParameters, null), startOffset, maxResults, expandedQuery);
   }

   /**
    * Make a new FilterParsingResult after normalizing the query. This FilterParsingResult is not supposed to have
    * grouping/aggregation.
    */
   private IckleParsingResult<TypeMetadata> makeFilterParsingResult(IckleParsingResult<TypeMetadata> parsingResult, BooleanExpr normalizedWhereClause,
                                                                    PropertyPath[] projection, Class<?>[] projectedTypes, SortField[] sortFields) {
      String queryString = SyntaxTreePrinter.printTree(parsingResult.getTargetEntityName(), projection, normalizedWhereClause, sortFields);
      return new IckleParsingResult<>(queryString, parsingResult.getParameterNames(),
            normalizedWhereClause, null,
            parsingResult.getTargetEntityName(), parsingResult.getTargetEntityMetadata(),
            projection, projectedTypes, null, sortFields);
   }

   protected ResultProcessor<?, ?> makeResultProcessor(ResultProcessor<?, ?> in) {
      return in;
   }

   protected RowProcessor makeProjectionProcessor(Class<?>[] projectedTypes) {
      return null;
   }

   protected IckleParsingResult<TypeMetadata> parse(String queryString) {
      return queryCache != null
            ? queryCache.get(queryString, null, IckleParsingResult.class, (qs, accumulators) -> IckleParser.parse(qs, propertyHelper))
            : IckleParser.parse(queryString, propertyHelper);
   }

   private ObjectFilter getObjectFilter(Matcher matcher, String queryString, Map<String, Object> namedParameters, List<FieldAccumulator> accumulators) {
      ObjectFilter objectFilter = queryCache != null
            ? queryCache.get(queryString, accumulators, matcher.getClass(), matcher::getObjectFilter)
            : matcher.getObjectFilter(queryString, accumulators);
      return namedParameters != null ? objectFilter.withParameters(namedParameters) : objectFilter;
   }

   protected final IckleFilterAndConverter createAndWireFilter(String queryString, Map<String, Object> namedParameters) {
      IckleFilterAndConverter filter = createFilter(queryString, namedParameters);

      SecurityActions.doPrivileged(() -> {
         cache.getComponentRegistry().wireDependencies(filter);
         return null;
      });

      return filter;
   }

   protected IckleFilterAndConverter createFilter(String queryString, Map<String, Object> namedParameters) {
      return new IckleFilterAndConverter(queryString, namedParameters, matcherImplClass);
   }

   /**
    * Build a Lucene index query.
    */
   protected <E> CacheQuery<E> buildLuceneQuery(IckleParsingResult<TypeMetadata> ickleParsingResult, Map<String, Object> namedParameters, long startOffset, int maxResults) {
      if (log.isDebugEnabled()) {
         log.debugf("Building Lucene query for : %s", ickleParsingResult.getQueryString());
      }

      if (!isIndexed) {
         throw log.cannotRunLuceneQueriesIfNotIndexed(cache.getName());
      }

      LuceneQueryParsingResult luceneParsingResult = transformParsingResult(ickleParsingResult, namedParameters);
      org.apache.lucene.search.Query luceneQuery = makeTypeQuery(luceneParsingResult.getQuery(), luceneParsingResult.getTargetEntityName());

      if (log.isDebugEnabled()) {
         log.debugf("The resulting Lucene query is : %s", luceneQuery.toString());
      }

      CacheQuery<?> cacheQuery = makeCacheQuery(ickleParsingResult, luceneQuery);
      if (luceneParsingResult.getSort() != null) {
         cacheQuery = cacheQuery.sort(luceneParsingResult.getSort());
      }
      if (luceneParsingResult.getProjections() != null) {
         cacheQuery = cacheQuery.projection(luceneParsingResult.getProjections());
      }
      if (startOffset >= 0) {
         cacheQuery = cacheQuery.firstResult((int) startOffset);
      }
      if (maxResults > 0) {
         cacheQuery = cacheQuery.maxResults(maxResults);
      }

      return (CacheQuery<E>) cacheQuery;
   }

   private LuceneQueryParsingResult<TypeMetadata> transformParsingResult(IckleParsingResult<TypeMetadata> parsingResult, Map<String, Object> namedParameters) {
      return queryCache != null && parsingResult.getParameterNames().isEmpty()
            ? queryCache.get(parsingResult.getQueryString(), null, LuceneQueryParsingResult.class, (queryString, accumulators) -> transformToLuceneQueryParsingResult(parsingResult, namedParameters))
            : transformToLuceneQueryParsingResult(parsingResult, namedParameters);
   }

   private LuceneQueryParsingResult<TypeMetadata> transformToLuceneQueryParsingResult(IckleParsingResult<TypeMetadata> parsingResult, Map<String, Object> namedParameters) {
      return new LuceneQueryMaker<>(getSearchFactory(), fieldBridgeAndAnalyzerProvider)
            .transform(parsingResult, namedParameters, getTargetedClass(parsingResult));
   }

   protected org.apache.lucene.search.Query makeTypeQuery(org.apache.lucene.search.Query query, String targetEntityName) {
      return query;
   }

   protected Class<?> getTargetedClass(IckleParsingResult<?> parsingResult) {
      return (Class<?>) parsingResult.getTargetEntityMetadata();
   }

   protected CacheQuery<?> makeCacheQuery(IckleParsingResult<TypeMetadata> ickleParsingResult, org.apache.lucene.search.Query luceneQuery) {
      return getSearchManager().getQuery(luceneQuery, getTargetedClass(ickleParsingResult));
   }
}
