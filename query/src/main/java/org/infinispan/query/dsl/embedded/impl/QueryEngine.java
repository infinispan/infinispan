package org.infinispan.query.dsl.embedded.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.hibernate.hql.QueryParser;
import org.hibernate.hql.ast.spi.EntityNamesResolver;
import org.hibernate.hql.lucene.LuceneProcessingChain;
import org.hibernate.hql.lucene.LuceneQueryParsingResult;
import org.hibernate.hql.lucene.internal.builder.ClassBasedLucenePropertyHelper;
import org.hibernate.hql.lucene.spi.FieldBridgeProvider;
import org.hibernate.search.bridge.FieldBridge;
import org.hibernate.search.spi.SearchIntegrator;
import org.infinispan.AdvancedCache;
import org.infinispan.commons.util.Util;
import org.infinispan.objectfilter.ObjectFilter;
import org.infinispan.objectfilter.PropertyPath;
import org.infinispan.objectfilter.SortField;
import org.infinispan.objectfilter.impl.BaseMatcher;
import org.infinispan.objectfilter.impl.ReflectionMatcher;
import org.infinispan.objectfilter.impl.RowMatcher;
import org.infinispan.objectfilter.impl.aggregation.FieldAccumulator;
import org.infinispan.objectfilter.impl.hql.FilterParsingResult;
import org.infinispan.objectfilter.impl.hql.ObjectPropertyHelper;
import org.infinispan.objectfilter.impl.hql.RowPropertyHelper;
import org.infinispan.objectfilter.impl.syntax.AggregationExpr;
import org.infinispan.objectfilter.impl.syntax.AndExpr;
import org.infinispan.objectfilter.impl.syntax.BooleShannonExpansion;
import org.infinispan.objectfilter.impl.syntax.BooleanExpr;
import org.infinispan.objectfilter.impl.syntax.BooleanFilterNormalizer;
import org.infinispan.objectfilter.impl.syntax.ComparisonExpr;
import org.infinispan.objectfilter.impl.syntax.ConstantBooleanExpr;
import org.infinispan.objectfilter.impl.syntax.ConstantValueExpr;
import org.infinispan.objectfilter.impl.syntax.IsNullExpr;
import org.infinispan.objectfilter.impl.syntax.JPATreePrinter;
import org.infinispan.objectfilter.impl.syntax.LikeExpr;
import org.infinispan.objectfilter.impl.syntax.NotExpr;
import org.infinispan.objectfilter.impl.syntax.OrExpr;
import org.infinispan.objectfilter.impl.syntax.PropertyValueExpr;
import org.infinispan.objectfilter.impl.syntax.ValueExpr;
import org.infinispan.objectfilter.impl.syntax.Visitor;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.impl.BaseQuery;
import org.infinispan.query.dsl.impl.JPAQueryGenerator;
import org.infinispan.query.impl.ComponentRegistryUtils;
import org.infinispan.query.logging.Log;
import org.infinispan.security.AuthorizationManager;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.util.KeyValuePair;
import org.infinispan.util.logging.LogFactory;

/**
 * @author anistor@redhat.com
 * @since 7.2
 */
public class QueryEngine {

   private static final Log log = LogFactory.getLog(QueryEngine.class, Log.class);

   private static final int MAX_EXPANSION_COFACTORS = 16;

   private final AuthorizationManager authorizationManager;

   protected final AdvancedCache<?, ?> cache;

   protected final boolean isIndexed;

   /**
    * Optional cache for query objects.
    */
   private final QueryCache queryCache;

   /**
    * Optional, lazily acquired. This is {@code null} if the cache is not indexed.
    */
   private SearchManager searchManager;

   /**
    * Optional, lazily acquired. This is {@code null} if the cache is not indexed.
    */
   private SearchIntegrator searchFactory;

   private final QueryParser queryParser = new QueryParser();

   private final BooleanFilterNormalizer booleanFilterNormalizer = new BooleanFilterNormalizer();

   public QueryEngine(AdvancedCache<?, ?> cache, boolean isIndexed) {
      this.cache = cache;
      this.isIndexed = isIndexed;
      this.queryCache = ComponentRegistryUtils.getQueryCache(cache);
      authorizationManager = SecurityActions.getCacheAuthorizationManager(cache);
   }

   private SearchManager getSearchManager() {
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

   public BaseQuery buildQuery(QueryFactory queryFactory, String jpqlString, Map<String, Object> namedParameters, long startOffset, int maxResults) {
      if (log.isDebugEnabled()) {
         log.debugf("Building query for : %s", jpqlString);
      }

      if (authorizationManager != null) {
         authorizationManager.checkPermission(AuthorizationPermission.BULK_READ);
      }

      checkParameters(namedParameters);

      FilterParsingResult<?> parsingResult = parse(jpqlString, namedParameters);
      if (parsingResult.hasGroupingOrAggregations()) {
         return buildQueryWithAggregations(queryFactory, jpqlString, namedParameters, startOffset, maxResults, parsingResult);
      }
      return buildQueryNoAggregations(queryFactory, jpqlString, namedParameters, startOffset, maxResults, parsingResult);
   }

   /**
    * Ensure all parameters have non-null values.
    */
   private void checkParameters(Map<String, Object> namedParameters) {
      if (namedParameters != null) {
         for (Map.Entry<String, Object> e : namedParameters.entrySet()) {
            if (e.getValue() == null) {
               throw log.queryParameterNotSet(e.getKey());
            }
         }
      }
   }

   private BaseQuery buildQueryWithAggregations(QueryFactory queryFactory, String jpqlString, Map<String, Object> namedParameters, long startOffset, int maxResults, FilterParsingResult<?> parsingResult) {
      if (parsingResult.getProjectedPaths() == null) {
         throw log.groupingAndAggregationQueriesMustUseProjections();
      }

      LinkedHashMap<PropertyPath, RowPropertyHelper.ColumnMetadata> columns = new LinkedHashMap<>();

      final ObjectPropertyHelper<?> propertyHelper = getMatcher().getPropertyHelper();
      if (parsingResult.getGroupBy() != null) {
         for (PropertyPath p : parsingResult.getGroupBy()) {
            if (p.getAggregationType() != null) {
               throw log.cannotHaveAggregationsInGroupByClause();  // should not really be possible because this was validated during parsing
            }
            // Duplicates in 'group by' are accepted and silently discarded. This behaviour is similar to SQL.
            if (!columns.containsKey(p)) {
               if (propertyHelper.isRepeatedProperty(parsingResult.getTargetEntityName(), p.getPath())) {
                  // this constraint will be relaxed later: https://issues.jboss.org/browse/ISPN-6015
                  throw log.multivaluedPropertyCannotBeUsedInGroupBy(p.toString());
               }
               Class<?> propertyType = propertyHelper.getPrimitivePropertyType(parsingResult.getTargetEntityName(), p.getPath());
               int idx = columns.size();
               columns.put(p, new RowPropertyHelper.ColumnMetadata(idx, "C" + idx, propertyType));
            }
         }
      }
      final int noOfGroupingColumns = columns.size();

      for (int i = 0; i < parsingResult.getProjectedPaths().length; i++) {
         PropertyPath p = parsingResult.getProjectedPaths()[i];
         RowPropertyHelper.ColumnMetadata c = columns.get(p);
         if (p.getAggregationType() == null) {
            // this must be an already processed 'group by' field, or else it's an invalid query
            if (c == null || c.getColumnIndex() >= noOfGroupingColumns) {
               throw log.expressionMustBePartOfAggregateFunctionOrShouldBeIncludedInGroupByClause(p.toString());
            }
         }
         if (c == null) {
            Class<?> propertyType = parsingResult.getProjectedTypes()[i];
            if (p.getAggregationType() != null) {
               propertyType = FieldAccumulator.getOutputType(p.getAggregationType(), propertyType);
            }
            int idx = columns.size();
            columns.put(p, new RowPropertyHelper.ColumnMetadata(idx, "C" + idx, propertyType));
         }
      }
      if (parsingResult.getSortFields() != null) {
         for (SortField sortField : parsingResult.getSortFields()) {
            PropertyPath p = sortField.getPath();
            RowPropertyHelper.ColumnMetadata c = columns.get(p);
            if (p.getAggregationType() == null) {
               // this must be an already processed 'group by' field, or else it's an invalid query
               if (c == null || c.getColumnIndex() >= noOfGroupingColumns) {
                  throw log.expressionMustBePartOfAggregateFunctionOrShouldBeIncludedInGroupByClause(p.toString());
               }
            }
            if (c == null) {
               Class<?> propertyType = propertyHelper.getPrimitivePropertyType(parsingResult.getTargetEntityName(), p.getPath());
               int idx = columns.size();
               columns.put(p, new RowPropertyHelper.ColumnMetadata(idx, "C" + idx, propertyType));
            }
         }
      }

      String havingClause = null;
      if (parsingResult.getHavingClause() != null) {
         BooleanExpr normalizedHavingClause = booleanFilterNormalizer.normalize(parsingResult.getHavingClause());
         if (normalizedHavingClause == ConstantBooleanExpr.FALSE) {
            return new EmptyResultQuery(queryFactory, cache, jpqlString, namedParameters, startOffset, maxResults);
         }
         if (normalizedHavingClause != ConstantBooleanExpr.TRUE) {
            havingClause = JPATreePrinter.printTree(swapVariables(normalizedHavingClause, parsingResult.getTargetEntityName(), columns));
         }
      }

      // validity of query is established at this point, no more checks needed

      for (PropertyPath p : columns.keySet()) {
         if (propertyHelper.isRepeatedProperty(parsingResult.getTargetEntityName(), p.getPath())) {
            return buildQueryWithRepeatedAggregations(queryFactory, jpqlString, namedParameters, startOffset, maxResults,
                  parsingResult, havingClause, columns, noOfGroupingColumns);
         }
      }

      LinkedHashMap<String, Integer> inColumns = new LinkedHashMap<>();
      List<FieldAccumulator> accumulators = new LinkedList<>();
      RowPropertyHelper.ColumnMetadata[] _columns = new RowPropertyHelper.ColumnMetadata[columns.size()];
      for (PropertyPath p : columns.keySet()) {
         RowPropertyHelper.ColumnMetadata c = columns.get(p);
         _columns[c.getColumnIndex()] = c;
         String asStringPath = p.asStringPath();
         Integer inIdx = inColumns.get(asStringPath);
         if (inIdx == null) {
            inIdx = inColumns.size();
            inColumns.put(asStringPath, inIdx);
         }
         if (p.getAggregationType() != null) {
            FieldAccumulator acc = FieldAccumulator.makeAccumulator(p.getAggregationType(), inIdx, c.getColumnIndex(), c.getPropertyType());
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
            firstPhaseQuery.append(JPAQueryGenerator.DEFAULT_ALIAS).append('.').append(p);
         }
      }
      firstPhaseQuery.append(" FROM ").append(parsingResult.getTargetEntityName()).append(' ').append(JPAQueryGenerator.DEFAULT_ALIAS);
      if (parsingResult.getWhereClause() != null) {
         // the WHERE clause should not touch aggregated fields
         BooleanExpr normalizedWhereClause = booleanFilterNormalizer.normalize(parsingResult.getWhereClause());
         if (normalizedWhereClause == ConstantBooleanExpr.FALSE) {
            return new EmptyResultQuery(queryFactory, cache, jpqlString, namedParameters, startOffset, maxResults);
         }
         if (normalizedWhereClause != ConstantBooleanExpr.TRUE) {
            firstPhaseQuery.append(' ').append(JPATreePrinter.printTree(normalizedWhereClause));
         }
      }

      StringBuilder secondPhaseQuery = new StringBuilder();
      secondPhaseQuery.append("SELECT ");
      {
         for (int i = 0; i < parsingResult.getProjectedPaths().length; i++) {
            PropertyPath p = parsingResult.getProjectedPaths()[i];
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
      BaseQuery baseQuery = buildQueryNoAggregations(queryFactory, firstPhaseQueryStr, namedParameters, -1, -1, parse(firstPhaseQueryStr, namedParameters));

      // second phase: grouping, aggregation, 'having' clause filtering, sorting and pagination
      String secondPhaseQueryStr = secondPhaseQuery.toString();
      return new AggregatingQuery(queryFactory, cache, secondPhaseQueryStr, namedParameters,
            noOfGroupingColumns, accumulators, false,
            getObjectFilter(new RowMatcher(_columns), secondPhaseQueryStr, namedParameters, null),
            startOffset, maxResults, baseQuery);
   }

   /**
    * Swaps all occurrences of PropertyPaths in given expression tree (the HAVING clause) with new PropertyPaths according to the mapping
    * found in {@code columns} map.
    */
   private BooleanExpr swapVariables(final BooleanExpr expr, final String targetEntityName,
                                     final LinkedHashMap<PropertyPath, RowPropertyHelper.ColumnMetadata> columns) {
      final ObjectPropertyHelper<?> propertyHelper = getMatcher().getPropertyHelper();
      class PropertyReplacer implements Visitor {

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
            return new LikeExpr(likeExpr.getChild().acceptVisitor(this), likeExpr.getPattern());
         }

         @Override
         public ValueExpr visit(ConstantValueExpr constantValueExpr) {
            return constantValueExpr;
         }

         @Override
         public ValueExpr visit(PropertyValueExpr propertyValueExpr) {
            PropertyPath p = new PropertyPath(null, propertyValueExpr.getPropertyPath());
            RowPropertyHelper.ColumnMetadata c = columns.get(p);
            if (c == null) {
               throw log.expressionMustBePartOfAggregateFunctionOrShouldBeIncludedInGroupByClause(propertyValueExpr.toJpaString());
            }
            return new PropertyValueExpr(c.getColumnName(), propertyValueExpr.isRepeated());
         }

         @Override
         public ValueExpr visit(AggregationExpr aggregationExpr) {
            PropertyPath p = new PropertyPath(aggregationExpr.getAggregationType(), aggregationExpr.getPropertyPath());
            RowPropertyHelper.ColumnMetadata c = columns.get(p);
            if (c == null) {
               Class<?> propertyType = propertyHelper.getPrimitivePropertyType(targetEntityName, aggregationExpr.getPropertyPath());
               propertyType = FieldAccumulator.getOutputType(aggregationExpr.getAggregationType(), propertyType);
               int idx = columns.size();
               c = new RowPropertyHelper.ColumnMetadata(idx, "C" + idx, propertyType);
               columns.put(p, c);
            }
            return new PropertyValueExpr(c.getColumnName(), aggregationExpr.isRepeated());
         }
      }
      return expr.acceptVisitor(new PropertyReplacer());
   }

   private BaseQuery buildQueryWithRepeatedAggregations(QueryFactory queryFactory, String jpqlString, Map<String, Object> namedParameters, long startOffset, int maxResults,
                                                        FilterParsingResult<?> parsingResult, String havingClause,
                                                        LinkedHashMap<PropertyPath, RowPropertyHelper.ColumnMetadata> columns, int noOfGroupingColumns) {
      // these types of aggregations can only be computed in memory

      final ObjectPropertyHelper<?> propertyHelper = getMatcher().getPropertyHelper();

      StringBuilder firstPhaseQuery = new StringBuilder();
      firstPhaseQuery.append("FROM ").append(parsingResult.getTargetEntityName()).append(' ').append(JPAQueryGenerator.DEFAULT_ALIAS);
      if (parsingResult.getWhereClause() != null) {
         // the WHERE clause should not touch aggregated fields
         BooleanExpr normalizedWhereClause = booleanFilterNormalizer.normalize(parsingResult.getWhereClause());
         if (normalizedWhereClause == ConstantBooleanExpr.FALSE) {
            return new EmptyResultQuery(queryFactory, cache, jpqlString, namedParameters, startOffset, maxResults);
         }
         if (normalizedWhereClause != ConstantBooleanExpr.TRUE) {
            firstPhaseQuery.append(' ').append(JPATreePrinter.printTree(normalizedWhereClause));
         }
      }
      String firstPhaseQueryStr = firstPhaseQuery.toString();
      BaseQuery baseQuery = buildQueryNoAggregations(queryFactory, firstPhaseQueryStr, namedParameters, -1, -1, parse(firstPhaseQueryStr, namedParameters));

      List<FieldAccumulator> secondPhaseAccumulators = new LinkedList<>();
      List<FieldAccumulator> thirdPhaseAccumulators = new LinkedList<>();
      RowPropertyHelper.ColumnMetadata[] _columns = new RowPropertyHelper.ColumnMetadata[columns.size()];
      StringBuilder secondPhaseQuery = new StringBuilder();
      secondPhaseQuery.append("SELECT ");
      for (PropertyPath p : columns.keySet()) {
         RowPropertyHelper.ColumnMetadata c = columns.get(p);
         if (c.getColumnIndex() > 0) {
            secondPhaseQuery.append(", ");
         }
         // only multi-valued fields need to be accumulated in this phase; for the others the accumulator is null
         if (p.getAggregationType() != null) {
            FieldAccumulator acc = FieldAccumulator.makeAccumulator(p.getAggregationType(), c.getColumnIndex(), c.getColumnIndex(), c.getPropertyType());
            if (propertyHelper.isRepeatedProperty(parsingResult.getTargetEntityName(), p.getPath())) {
               secondPhaseAccumulators.add(acc);
               if (p.getAggregationType() == PropertyPath.AggregationType.COUNT) {
                  c = new RowPropertyHelper.ColumnMetadata(c.getColumnIndex(), c.getColumnName(), Long.class);
                  acc = FieldAccumulator.makeAccumulator(PropertyPath.AggregationType.SUM, c.getColumnIndex(), c.getColumnIndex(), Long.class);
               }
            } else {
               secondPhaseAccumulators.add(null);
            }
            thirdPhaseAccumulators.add(acc);
         } else {
            secondPhaseAccumulators.add(null);
         }
         secondPhaseQuery.append(JPAQueryGenerator.DEFAULT_ALIAS).append('.').append(p.asStringPath());
         _columns[c.getColumnIndex()] = c;
      }
      secondPhaseQuery.append(" FROM ").append(parsingResult.getTargetEntityName()).append(' ').append(JPAQueryGenerator.DEFAULT_ALIAS);
      String secondPhaseQueryStr = secondPhaseQuery.toString();

      HybridQuery projectingAggregatingQuery = new HybridQuery(queryFactory, cache,
            secondPhaseQueryStr, namedParameters,
            getObjectFilter(getMatcher(), secondPhaseQueryStr, namedParameters, secondPhaseAccumulators),
            -1, -1, baseQuery);

      StringBuilder thirdPhaseQuery = new StringBuilder();
      thirdPhaseQuery.append("SELECT ");
      for (int i = 0; i < parsingResult.getProjectedPaths().length; i++) {
         PropertyPath p = parsingResult.getProjectedPaths()[i];
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

   private BaseQuery buildQueryNoAggregations(QueryFactory queryFactory, String jpqlString, Map<String, Object> namedParameters,
                                              long startOffset, int maxResults, FilterParsingResult<?> parsingResult) {
      if (parsingResult.hasGroupingOrAggregations()) {
         throw log.queryMustNotUseGroupingOrAggregation(); // may happen only due to internal programming error
      }

      final ObjectPropertyHelper<?> propertyHelper = getMatcher().getPropertyHelper();

      if (parsingResult.getSortFields() != null) {
         for (SortField sortField : parsingResult.getSortFields()) {
            PropertyPath p = sortField.getPath();
            if (propertyHelper.isRepeatedProperty(parsingResult.getTargetEntityName(), p.getPath())) {
               throw log.multivaluedPropertyCannotBeUsedInOrderBy(p.toString());
            }
         }
      }

      if (parsingResult.getProjectedPaths() != null) {
         for (PropertyPath p : parsingResult.getProjectedPaths()) {
            if (propertyHelper.isRepeatedProperty(parsingResult.getTargetEntityName(), p.getPath())) {
               throw log.multivaluedPropertyCannotBeProjected(p.asStringPath());
            }
         }
      }

      BooleanExpr normalizedWhereClause = booleanFilterNormalizer.normalize(parsingResult.getWhereClause());
      if (normalizedWhereClause == ConstantBooleanExpr.FALSE) {
         // the query is a contradiction, there are no matches
         return new EmptyResultQuery(queryFactory, cache, jpqlString, namedParameters, startOffset, maxResults);
      }

      if (!isIndexed) {
         return new EmbeddedQuery(this, queryFactory, cache, jpqlString, namedParameters, parsingResult.getProjections(), startOffset, maxResults);
      }

      BooleShannonExpansion.IndexedFieldProvider indexedFieldProvider = getIndexedFieldProvider(parsingResult);

      boolean allProjectionsAreStored = true;
      LinkedHashMap<PropertyPath, List<Integer>> projectionsMap = null;
      if (parsingResult.getProjectedPaths() != null) {
         projectionsMap = new LinkedHashMap<>();
         for (int i = 0; i < parsingResult.getProjectedPaths().length; i++) {
            PropertyPath p = parsingResult.getProjectedPaths()[i];
            List<Integer> idx = projectionsMap.get(p);
            if (idx == null) {
               idx = new ArrayList<>();
               projectionsMap.put(p, idx);
               if (!indexedFieldProvider.isStored(p.getPath())) {
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
            PropertyPath p = sf.getPath();
            String asStringPath = p.asStringPath();
            if (!sortFieldMap.containsKey(asStringPath)) {
               sortFieldMap.put(asStringPath, sf);
               if (!indexedFieldProvider.isStored(p.getPath())) {
                  allSortFieldsAreStored = false;
               }
            }
         }
         sortFields = sortFieldMap.values().toArray(new SortField[sortFieldMap.size()]);
      }

      BooleShannonExpansion bse = new BooleShannonExpansion(MAX_EXPANSION_COFACTORS, indexedFieldProvider);
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
                     PropertyPath[] deduplicated = projectionsMap.keySet().toArray(new PropertyPath[projectionsMap.size()]);
                     jpqlString = JPATreePrinter.printTree(parsingResult.getTargetEntityName(), deduplicated, normalizedWhereClause, sortFields);
                     return new EmbeddedLuceneQuery(this, queryFactory, jpqlString, namedParameters, parsingResult.getProjections(), makeResultProcessor(rowProcessor), startOffset, maxResults);
                  } else {
                     rowProcessor = makeProjectionProcessor(parsingResult.getProjectedTypes());
                  }
               }
               return new EmbeddedLuceneQuery(this, queryFactory, jpqlString, namedParameters, parsingResult.getProjections(), makeResultProcessor(rowProcessor), startOffset, maxResults);
            } else {
               String indexQueryStr = JPATreePrinter.printTree(parsingResult.getTargetEntityName(), null, normalizedWhereClause, sortFields);
               Query indexQuery = new EmbeddedLuceneQuery(this, queryFactory, indexQueryStr, namedParameters, null, makeResultProcessor(null), startOffset, maxResults);
               String projectionQueryStr = JPATreePrinter.printTree(parsingResult.getTargetEntityName(), parsingResult.getProjectedPaths(), null, null);
               return new HybridQuery(queryFactory, cache, projectionQueryStr, null, getObjectFilter(getMatcher(), projectionQueryStr, null, null), -1, -1, indexQuery);
            }
         } else {
            // projections may be stored but some sort fields are not so we need to query the index and then execute in-memory sorting and projecting in a second phase
            String indexQueryStr = JPATreePrinter.printTree(parsingResult.getTargetEntityName(), null, normalizedWhereClause, null);
            Query indexQuery = new EmbeddedLuceneQuery(this, queryFactory, indexQueryStr, namedParameters, null, makeResultProcessor(null), -1, -1);
            String projectionQueryStr = JPATreePrinter.printTree(parsingResult.getTargetEntityName(), parsingResult.getProjectedPaths(), null, sortFields);
            return new HybridQuery(queryFactory, cache, projectionQueryStr, null, getObjectFilter(getMatcher(), projectionQueryStr, null, null), startOffset, maxResults, indexQuery);
         }
      }

      if (expansion == ConstantBooleanExpr.TRUE) {
         // expansion leads to a full non-indexed query or the expansion is too long/complex
         return new EmbeddedQuery(this, queryFactory, cache, jpqlString, namedParameters, parsingResult.getProjections(), startOffset, maxResults);
      }

      // some fields are indexed, run a hybrid query
      String expandedQueryStr = JPATreePrinter.printTree(parsingResult.getTargetEntityName(), null, expansion, null);
      Query expandedQuery = new EmbeddedLuceneQuery(this, queryFactory, expandedQueryStr, namedParameters, null, makeResultProcessor(null), -1, -1);
      return new HybridQuery(queryFactory, cache, jpqlString, namedParameters, getObjectFilter(getMatcher(), jpqlString, namedParameters, null), startOffset, maxResults, expandedQuery);
   }

   protected ResultProcessor makeResultProcessor(ResultProcessor in) {
      return in;
   }

   protected RowProcessor makeProjectionProcessor(Class<?>[] projectedTypes) {
      return null;
   }

   protected BaseMatcher getMatcher() {
      return SecurityActions.getCacheComponentRegistry(cache).getComponent(ReflectionMatcher.class);
   }

   private FilterParsingResult<?> parse(String jpqlString, Map<String, Object> namedParameters) {
      FilterParsingResult<?> parsingResult;
      // if parameters are present caching cannot be currently performed due to internal implementation limitations
      if (queryCache != null && (namedParameters == null || namedParameters.isEmpty())) {
         KeyValuePair<String, Class> queryCacheKey = new KeyValuePair<>(jpqlString, FilterParsingResult.class);
         parsingResult = queryCache.get(queryCacheKey);
         if (parsingResult == null) {
            parsingResult = getMatcher().parse(jpqlString, namedParameters);
            queryCache.put(queryCacheKey, parsingResult);
         }
      } else {
         parsingResult = getMatcher().parse(jpqlString, namedParameters);
      }
      return parsingResult;
   }

   private ObjectFilter getObjectFilter(BaseMatcher matcher, String jpqlString, Map<String, Object> namedParameters, List<FieldAccumulator> acc) {
      ObjectFilter objectFilter;
      // if parameters are present caching cannot be currently performed due to internal implementation limitations
      if (queryCache != null && (namedParameters == null || namedParameters.isEmpty())) {
         KeyValuePair<String, KeyValuePair<Class, List<FieldAccumulator>>> queryCacheKey = new KeyValuePair<>(jpqlString, new KeyValuePair<>(matcher.getClass(), acc));
         objectFilter = queryCache.get(queryCacheKey);
         if (objectFilter == null) {
            objectFilter = matcher.getObjectFilter(jpqlString, namedParameters, acc);
            queryCache.put(queryCacheKey, objectFilter);
         }
      } else {
         objectFilter = matcher.getObjectFilter(jpqlString, namedParameters, acc);
      }
      return objectFilter;
   }

   protected BooleShannonExpansion.IndexedFieldProvider getIndexedFieldProvider(FilterParsingResult<?> parsingResult) {
      return new HibernateSearchIndexedFieldProvider(getSearchFactory(), (Class<?>) parsingResult.getTargetEntityMetadata());
   }

   protected JPAFilterAndConverter makeFilter(String jpaQuery, Map<String, Object> namedParameters) {
      final JPAFilterAndConverter filter = createFilter(jpaQuery, namedParameters);

      SecurityActions.doPrivileged(() -> {
         cache.getComponentRegistry().wireDependencies(filter);
         return null;
      });

      return filter;
   }

   protected JPAFilterAndConverter createFilter(String jpaQuery, Map<String, Object> namedParameters) {
      return new JPAFilterAndConverter(jpaQuery, namedParameters, ReflectionMatcher.class);
   }

   /**
    * Build a Lucene index query.
    */
   protected CacheQuery buildLuceneQuery(String jpqlString, Map<String, Object> namedParameters, long startOffset, int maxResults) {
      if (log.isDebugEnabled()) {
         log.debugf("Building Lucene query for : %s", jpqlString);
      }

      if (!isIndexed) {
         throw log.cannotRunLuceneQueriesIfNotIndexed();
      }

      checkParameters(namedParameters);

      LuceneQueryParsingResult parsingResult = transformJpaToLucene(jpqlString, namedParameters);
      org.apache.lucene.search.Query luceneQuery = makeTypeQuery(parsingResult.getQuery(), parsingResult.getTargetEntityName());
      CacheQuery cacheQuery = getSearchManager().getQuery(luceneQuery, parsingResult.getTargetEntity());

      if (parsingResult.getSort() != null) {
         cacheQuery = cacheQuery.sort(parsingResult.getSort());
      }
      if (parsingResult.getProjections() != null && !parsingResult.getProjections().isEmpty()) {
         String[] projection = parsingResult.getProjections().toArray(new String[parsingResult.getProjections().size()]);
         cacheQuery = cacheQuery.projection(projection);
      }
      if (startOffset >= 0) {
         cacheQuery = cacheQuery.firstResult((int) startOffset);
      }
      if (maxResults > 0) {
         cacheQuery = cacheQuery.maxResults(maxResults);
      }

      return cacheQuery;
   }

   protected org.apache.lucene.search.Query makeTypeQuery(org.apache.lucene.search.Query query, String targetEntityName) {
      return query;
   }

   private LuceneQueryParsingResult transformJpaToLucene(String jpqlString, Map<String, Object> namedParameters) {
      LuceneQueryParsingResult parsingResult;
      // if parameters are present caching cannot be currently performed due to internal implementation limitations
      if (queryCache != null && (namedParameters == null || namedParameters.isEmpty())) {
         KeyValuePair<String, Class> queryCacheKey = new KeyValuePair<>(jpqlString, LuceneQueryParsingResult.class);
         parsingResult = queryCache.get(queryCacheKey);
         if (parsingResult == null) {
            parsingResult = queryParser.parseQuery(jpqlString, makeParsingProcessingChain(namedParameters));
            queryCache.put(queryCacheKey, parsingResult);
         }
      } else {
         parsingResult = queryParser.parseQuery(jpqlString, makeParsingProcessingChain(namedParameters));
      }
      return parsingResult;
   }

   protected LuceneProcessingChain makeParsingProcessingChain(Map<String, Object> namedParameters) {
      final EntityNamesResolver entityNamesResolver = new EntityNamesResolver() {
         @Override
         public Class<?> getClassFromName(String entityName) {
            try {
               return Util.loadClassStrict(entityName, null);
            } catch (ClassNotFoundException e) {
               return null;
            }
         }
      };

      FieldBridgeProvider fieldBridgeProvider = new FieldBridgeProvider() {

         private final ClassBasedLucenePropertyHelper propertyHelper = new ClassBasedLucenePropertyHelper(getSearchFactory(), entityNamesResolver);

         @Override
         public FieldBridge getFieldBridge(String type, String propertyPath) {
            return propertyHelper.getFieldBridge(type, Arrays.asList(propertyPath.split("[.]")));
         }
      };
      return new LuceneProcessingChain.Builder(getSearchFactory(), entityNamesResolver)
            .namedParameters(namedParameters)
            .buildProcessingChainForClassBasedEntities(fieldBridgeProvider);
   }
}
