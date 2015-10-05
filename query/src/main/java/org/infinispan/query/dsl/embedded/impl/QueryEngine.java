package org.infinispan.query.dsl.embedded.impl;

import org.hibernate.hql.ParsingException;
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
import org.infinispan.objectfilter.impl.aggregation.AvgAccumulator;
import org.infinispan.objectfilter.impl.aggregation.CountAccumulator;
import org.infinispan.objectfilter.impl.aggregation.FieldAccumulator;
import org.infinispan.objectfilter.impl.aggregation.MaxAccumulator;
import org.infinispan.objectfilter.impl.aggregation.MinAccumulator;
import org.infinispan.objectfilter.impl.aggregation.SumAccumulator;
import org.infinispan.objectfilter.impl.hql.FilterParsingResult;
import org.infinispan.objectfilter.impl.hql.ObjectPropertyHelper;
import org.infinispan.objectfilter.impl.hql.RowPropertyHelper;
import org.infinispan.objectfilter.impl.syntax.*;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.SearchManager;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.impl.BaseQuery;
import org.infinispan.query.dsl.impl.JPAQueryGenerator;
import org.infinispan.query.impl.ComponentRegistryUtils;
import org.infinispan.util.KeyValuePair;

import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author anistor@redhat.com
 * @since 7.2
 */
public class QueryEngine {

   private static final int MAX_EXPANSION_COFACTORS = 16;

   protected final AdvancedCache<?, ?> cache;

   /**
    * Optional cache for query objects.
    */
   protected final QueryCache queryCache;

   /**
    * Optional. This is {@code null} if the cache is not indexed.
    */
   protected final SearchManager searchManager;

   /**
    * Optional. This is {@code null} if the cache is not indexed.
    */
   protected final SearchIntegrator searchFactory;

   protected final QueryParser queryParser = new QueryParser();

   protected final BooleanFilterNormalizer booleanFilterNormalizer = new BooleanFilterNormalizer();

   public QueryEngine(AdvancedCache<?, ?> cache, SearchManager searchManager) {
      this.cache = cache;
      this.queryCache = ComponentRegistryUtils.getQueryCache(cache);
      this.searchManager = searchManager;
      searchFactory = searchManager != null ? searchManager.unwrap(SearchIntegrator.class) : null;
   }

   public BaseQuery buildQuery(QueryFactory queryFactory, String jpqlString, Map<String, Object> namedParameters, long startOffset, int maxResults) {
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
               throw new IllegalStateException("Query parameter '" + e.getKey() + "' was not set");
            }
         }
      }
   }

   private BaseQuery buildQueryWithAggregations(QueryFactory queryFactory, String jpqlString, Map<String, Object> namedParameters, long startOffset, int maxResults, FilterParsingResult<?> parsingResult) {
      if (parsingResult.getProjectedPaths() == null) {
         throw new ParsingException("Queries containing grouping and aggregation functions must use projections.");
      }

      LinkedHashMap<PropertyPath, RowPropertyHelper.ColumnMetadata> columns = new LinkedHashMap<PropertyPath, RowPropertyHelper.ColumnMetadata>();

      ObjectPropertyHelper<?> propertyHelper = getFirstPhaseMatcher().getPropertyHelper();
      LinkedHashSet<Integer> groupFieldPositions = new LinkedHashSet<Integer>();
      if (parsingResult.getGroupBy() != null) {
         for (PropertyPath p : parsingResult.getGroupBy()) {
            // Duplicates in 'group by' are accepted and silently discarded. This behaviour is similar to SQL.
            if (!columns.containsKey(p)) {
               Class<?> propertyType = propertyHelper.getPrimitivePropertyType(parsingResult.getTargetEntityName(), p.getPath());
               int idx = columns.size();
               columns.put(p, new RowPropertyHelper.ColumnMetadata(idx, "C" + idx, propertyType));
               groupFieldPositions.add(idx);
            }
         }
      }
      for (PropertyPath p : parsingResult.getProjectedPaths()) {
         RowPropertyHelper.ColumnMetadata c = columns.get(p);
         if (p.getAggregationType() == null) {
            // this must be an already processed 'group by' field, or else it's an invalid query
            if (c == null || !groupFieldPositions.contains(c.getColumnIndex())) {
               throw new ParsingException("The expression '" + p + "' must be part of an aggregate function or it should be included in the GROUP BY clause");
            }
         }
         if (c == null) {
            Class<?> propertyType = propertyHelper.getPrimitivePropertyType(parsingResult.getTargetEntityName(), p.getPath());
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
               if (c == null || !groupFieldPositions.contains(c.getColumnIndex())) {
                  throw new ParsingException("The expression '" + p + "' must be part of an aggregate function or it should be included in the GROUP BY clause");
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

      StringBuilder firstPhaseQuery = new StringBuilder();
      firstPhaseQuery.append("SELECT ");
      {
         boolean isFirst = true;
         for (PropertyPath c : columns.keySet()) {
            if (isFirst) {
               isFirst = false;
            } else {
               firstPhaseQuery.append(", ");
            }
            firstPhaseQuery.append(JPAQueryGenerator.DEFAULT_ALIAS).append('.').append(c.asStringPath());
         }
      }
      firstPhaseQuery.append(" FROM ").append(parsingResult.getTargetEntityName()).append(' ').append(JPAQueryGenerator.DEFAULT_ALIAS);
      if (parsingResult.getWhereClause() != null) {
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
         boolean isFirst = true;
         for (PropertyPath p : parsingResult.getProjectedPaths()) {
            if (isFirst) {
               isFirst = false;
            } else {
               secondPhaseQuery.append(", ");
            }
            RowPropertyHelper.ColumnMetadata c = columns.get(p);
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

      List<FieldAccumulator> accumulators = new LinkedList<FieldAccumulator>();
      RowPropertyHelper.ColumnMetadata[] _columns = new RowPropertyHelper.ColumnMetadata[columns.size()];
      for (PropertyPath p : columns.keySet()) {
         RowPropertyHelper.ColumnMetadata c = columns.get(p);
         _columns[c.getColumnIndex()] = c;
         if (p.getAggregationType() != null) {
            switch (p.getAggregationType()) {
               case SUM:
                  accumulators.add(new SumAccumulator(c.getColumnIndex(), c.getPropertyType()));
                  break;
               case AVG:
                  accumulators.add(new AvgAccumulator(c.getColumnIndex(), c.getPropertyType()));
                  break;
               case MIN:
                  accumulators.add(new MinAccumulator(c.getColumnIndex(), c.getPropertyType()));
                  break;
               case MAX:
                  accumulators.add(new MaxAccumulator(c.getColumnIndex(), c.getPropertyType()));
                  break;
               case COUNT:
                  accumulators.add(new CountAccumulator(c.getColumnIndex()));
                  break;
               default:
                  throw new IllegalStateException("Aggregation " + p.getAggregationType().name() + " is not supported");
            }
         } else {
            groupFieldPositions.add(c.getColumnIndex());
         }
      }

      int[] _groupFieldPositions = new int[groupFieldPositions.size()];
      int i = 0;
      for (Integer pos : groupFieldPositions) {
         _groupFieldPositions[i++] = pos;
      }
      FieldAccumulator[] _accumulators = accumulators.toArray(new FieldAccumulator[accumulators.size()]);

      // first phase: gather rows matching the 'where' clause
      String firstPhaseQueryStr = firstPhaseQuery.toString();
      Query baseQuery = buildQueryNoAggregations(queryFactory, firstPhaseQueryStr, namedParameters, -1, -1, parse(firstPhaseQueryStr, namedParameters));

      // second phase: grouping, aggregation, 'having' clause filtering, sorting and paging
      String secondPhaseQueryStr = secondPhaseQuery.toString();
      return new AggregatingQuery(queryFactory, cache, secondPhaseQueryStr, namedParameters,
            _groupFieldPositions, _accumulators,
            getObjectFilter(new RowMatcher(_columns), secondPhaseQueryStr, namedParameters),
            startOffset, maxResults, baseQuery);
   }

   /**
    * Swaps all occurrences of PropertyPaths in given expression tree with new PropertyPaths according to the mapping
    * found in {@code colums} map.
    */
   private BooleanExpr swapVariables(final BooleanExpr expr, final String targetEntityName,
                                     final LinkedHashMap<PropertyPath, RowPropertyHelper.ColumnMetadata> columns) {
      final ObjectPropertyHelper<?> propertyHelper = getFirstPhaseMatcher().getPropertyHelper();
      class PropertyReplacer implements Visitor {

         @Override
         public BooleanExpr visit(NotExpr notExpr) {
            return new NotExpr(notExpr.getChild().acceptVisitor(this));
         }

         @Override
         public BooleanExpr visit(OrExpr orExpr) {
            List<BooleanExpr> visitedChildren = new ArrayList<BooleanExpr>();
            for (BooleanExpr c : orExpr.getChildren()) {
               visitedChildren.add(c.acceptVisitor(this));
            }
            return new OrExpr(visitedChildren);
         }

         @Override
         public BooleanExpr visit(AndExpr andExpr) {
            List<BooleanExpr> visitedChildren = new ArrayList<BooleanExpr>();
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
               throw new ParsingException("The expression '" + propertyValueExpr.toJpaString() + "' must be part of an aggregate function or it should be included in the GROUP BY clause");
            }
            return new PropertyValueExpr(c.getColumnName(), propertyValueExpr.isRepeated());
         }

         @Override
         public ValueExpr visit(AggregationExpr aggregationExpr) {
            PropertyPath p = new PropertyPath(aggregationExpr.getAggregationType(), aggregationExpr.getPropertyPath());
            RowPropertyHelper.ColumnMetadata c = columns.get(p);
            if (c == null) {
               Class<?> propertyType = propertyHelper.getPrimitivePropertyType(targetEntityName, aggregationExpr.getPropertyPath());
               if (aggregationExpr.getAggregationType() == PropertyPath.AggregationType.AVG) {
                  propertyType = Double.class;
               } else if (aggregationExpr.getAggregationType() == PropertyPath.AggregationType.COUNT) {
                  propertyType = Long.class;
               }
               int idx = columns.size();
               c = new RowPropertyHelper.ColumnMetadata(idx, "C" + idx, propertyType);
               columns.put(p, c);
            }
            return new PropertyValueExpr(c.getColumnName(), aggregationExpr.isRepeated());
         }
      }
      return expr.acceptVisitor(new PropertyReplacer());
   }

   private BaseQuery buildQueryNoAggregations(QueryFactory queryFactory, String jpqlString, Map<String, Object> namedParameters,
                                              long startOffset, int maxResults, FilterParsingResult<?> parsingResult) {
      if (parsingResult.hasGroupingOrAggregations()) {
         throw new IllegalArgumentException("The query must not use grouping or aggregation"); // may happen only due to programming error
      }

      BooleanExpr normalizedWhereClause = booleanFilterNormalizer.normalize(parsingResult.getWhereClause());
      if (normalizedWhereClause == ConstantBooleanExpr.FALSE) {
         // the query is a contradiction, there are no matches
         return new EmptyResultQuery(queryFactory, cache, jpqlString, namedParameters, startOffset, maxResults);
      }
      if (normalizedWhereClause == null || normalizedWhereClause == ConstantBooleanExpr.TRUE || searchManager == null) {
         return new EmbeddedQuery(this, queryFactory, cache, jpqlString, namedParameters, parsingResult.getProjections(), startOffset, maxResults);
      }

      BooleShannonExpansion bse = new BooleShannonExpansion(MAX_EXPANSION_COFACTORS, getIndexedFieldProvider(parsingResult));
      BooleanExpr expansion = bse.expand(normalizedWhereClause);

      if (expansion == normalizedWhereClause) {  // identity comparison is intended here!
         // everything is indexed, so go the Lucene way
         //todo [anistor] we should be able to generate the lucene query ourselves rather than depend on hibernate-hql-lucene to do it
         return new EmbeddedLuceneQuery(this, queryFactory, jpqlString, namedParameters, parsingResult.getProjections(), startOffset, maxResults);
      }

      if (expansion == ConstantBooleanExpr.TRUE) {
         // expansion leads to a full non-indexed query
         return new EmbeddedQuery(this, queryFactory, cache, jpqlString, namedParameters, parsingResult.getProjections(), startOffset, maxResults);
      }

      // some fields are indexed, run a hybrid query
      String expandedJpaOut = JPATreePrinter.printTree(parsingResult.getTargetEntityName(), expansion, null);
      Query expandedQuery = new EmbeddedLuceneQuery(this, queryFactory, expandedJpaOut, namedParameters, parsingResult.getProjections(), -1, -1);
      return new HybridQuery(queryFactory, cache, jpqlString, namedParameters, getObjectFilter(getSecondPhaseMatcher(), jpqlString, namedParameters),
            startOffset, maxResults, expandedQuery);
   }

   protected BaseMatcher getFirstPhaseMatcher() {
      return SecurityActions.getCacheComponentRegistry(cache).getComponent(ReflectionMatcher.class);
   }

   protected BaseMatcher getSecondPhaseMatcher() {
      return getFirstPhaseMatcher();
   }

   private FilterParsingResult<?> parse(String jpqlString, Map<String, Object> namedParameters) {
      FilterParsingResult<?> parsingResult;
      // if parameters are present caching cannot be currently performed due to internal implementation limitations
      if (queryCache != null && (namedParameters == null || namedParameters.isEmpty())) {
         KeyValuePair<String, Class> queryCacheKey = new KeyValuePair<String, Class>(jpqlString, FilterParsingResult.class);
         parsingResult = queryCache.get(queryCacheKey);
         if (parsingResult == null) {
            parsingResult = getFirstPhaseMatcher().parse(jpqlString, namedParameters);
            queryCache.put(queryCacheKey, parsingResult);
         }
      } else {
         parsingResult = getFirstPhaseMatcher().parse(jpqlString, namedParameters);
      }
      return parsingResult;
   }

   private ObjectFilter getObjectFilter(BaseMatcher matcher, String jpqlString, Map<String, Object> namedParameters) {
      ObjectFilter objectFilter;
      // if parameters are present caching cannot be currently performed due to internal implementation limitations
      if (queryCache != null && (namedParameters == null || namedParameters.isEmpty())) {
         KeyValuePair<String, Class> queryCacheKey = new KeyValuePair<String, Class>(jpqlString, matcher.getClass());
         objectFilter = queryCache.get(queryCacheKey);
         if (objectFilter == null) {
            objectFilter = matcher.getObjectFilter(jpqlString, namedParameters);
            queryCache.put(queryCacheKey, objectFilter);
         }
      } else {
         objectFilter = matcher.getObjectFilter(jpqlString, namedParameters);
      }
      return objectFilter;
   }

   protected BooleShannonExpansion.IndexedFieldProvider getIndexedFieldProvider(FilterParsingResult<?> parsingResult) {
      return new HibernateSearchIndexedFieldProvider(searchFactory, (Class<?>) parsingResult.getTargetEntityMetadata());
   }

   protected JPAFilterAndConverter makeFilter(String jpaQuery, Map<String, Object> namedParameters) {
      JPAFilterAndConverter filter = createFilter(jpaQuery, namedParameters);

      SecurityActions.doPrivileged(new PrivilegedAction<Object>() {
         @Override
         public Object run() {
            filter.injectDependencies(cache);
            return null;
         }
      });

      return filter;
   }

   protected JPAFilterAndConverter createFilter(String jpaQuery, Map<String, Object> namedParameters) {
      return new JPAFilterAndConverter(jpaQuery, namedParameters, ReflectionMatcher.class);
   }

   /**
    * Build a Lucene index query.
    */
   public CacheQuery buildLuceneQuery(String jpqlString, Map<String, Object> namedParameters, long startOffset, int maxResults) {
      if (searchManager == null) {
         throw new IllegalStateException("Cannot run Lucene queries on a cache that does not have indexing enabled");
      }

      checkParameters(namedParameters);

      LuceneQueryParsingResult parsingResult = transformJpaToLucene(jpqlString, namedParameters);
      org.apache.lucene.search.Query luceneQuery = makeTypeQuery(parsingResult.getQuery(), parsingResult.getTargetEntityName());
      CacheQuery cacheQuery = searchManager.getQuery(luceneQuery, parsingResult.getTargetEntity());

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
         KeyValuePair<String, Class> queryCacheKey = new KeyValuePair<String, Class>(jpqlString, LuceneQueryParsingResult.class);
         parsingResult = queryCache.get(queryCacheKey);
         if (parsingResult == null) {
            parsingResult = queryParser.parseQuery(jpqlString, makeProcessingChain(namedParameters));
            queryCache.put(queryCacheKey, parsingResult);
         }
      } else {
         parsingResult = queryParser.parseQuery(jpqlString, makeProcessingChain(namedParameters));
      }
      return parsingResult;
   }

   protected LuceneProcessingChain makeProcessingChain(Map<String, Object> namedParameters) {
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

         private final ClassBasedLucenePropertyHelper propertyHelper = new ClassBasedLucenePropertyHelper(searchFactory, entityNamesResolver);

         @Override
         public FieldBridge getFieldBridge(String type, String propertyPath) {
            return propertyHelper.getFieldBridge(type, Arrays.asList(propertyPath.split("[.]")));
         }
      };
      return new LuceneProcessingChain.Builder(searchFactory, entityNamesResolver)
            .namedParameters(namedParameters)
            .buildProcessingChainForClassBasedEntities(fieldBridgeProvider);
   }
}
