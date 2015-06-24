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
import org.infinispan.query.dsl.embedded.LuceneQuery;
import org.infinispan.query.impl.ComponentRegistryUtils;
import org.infinispan.util.KeyValuePair;

import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;

/**
 * @author anistor@redhat.com
 * @since 7.2
 */
public class QueryEngine {

   protected final AdvancedCache<?, ?> cache;

   /**
    * Optional cache for query objects.
    */
   protected final QueryCache queryCache;

   /**
    * Optional.
    */
   protected final SearchManager searchManager;

   /**
    * Optional.
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

   public Query buildQuery(QueryFactory queryFactory, String jpqlString, long startOffset, int maxResults) {
      FilterParsingResult<?> parsingResult = parse(jpqlString);
      if (parsingResult.hasGroupingOrAggregations()) {
         return buildQueryWithAggregations(queryFactory, jpqlString, startOffset, maxResults, parsingResult);
      }
      return buildQueryNoAggregations(queryFactory, jpqlString, startOffset, maxResults, parsingResult);
   }

   private Query buildQueryWithAggregations(QueryFactory queryFactory, String jpqlString, long startOffset, int maxResults, FilterParsingResult<?> parsingResult) {
      if (parsingResult.getProjectedPaths().isEmpty()) {
         throw new ParsingException("Queries containing grouping and aggregation functions must use projections.");
      }

      LinkedHashMap<PropertyPath, RowPropertyHelper.ColumnMetadata> columns = new LinkedHashMap<PropertyPath, RowPropertyHelper.ColumnMetadata>();

      ObjectPropertyHelper<?> propertyHelper = getFirstPhaseMatcher().getPropertyHelper();
      LinkedHashSet<Integer> groupFieldPositions = new LinkedHashSet<Integer>();
      for (PropertyPath p : parsingResult.getGroupBy()) {
         // Duplicates in 'group by' are accepted and silently discarded. This behaviour is similar to SQL.
         if (!columns.containsKey(p)) {
            int idx = columns.size();
            Class<?> propertyType = propertyHelper.getPrimitivePropertyType(parsingResult.getTargetEntityName(), p.getPath());
            columns.put(p, new RowPropertyHelper.ColumnMetadata(idx, "C" + idx, propertyType));
            groupFieldPositions.add(idx);
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
            int idx = columns.size();
            Class<?> propertyType = propertyHelper.getPrimitivePropertyType(parsingResult.getTargetEntityName(), p.getPath());
            columns.put(p, new RowPropertyHelper.ColumnMetadata(idx, "C" + idx, propertyType));
         }
      }
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
            int idx = columns.size();
            Class<?> propertyType = propertyHelper.getPrimitivePropertyType(parsingResult.getTargetEntityName(), p.getPath());
            columns.put(p, new RowPropertyHelper.ColumnMetadata(idx, "C" + idx, propertyType));
         }
      }

      String havingClause = null;
      if (parsingResult.getHavingClause() != null) {
         BooleanExpr normalizedHavingClause = booleanFilterNormalizer.normalize(parsingResult.getHavingClause());
         if (normalizedHavingClause == ConstantBooleanExpr.FALSE) {
            return new EmptyResultQuery(queryFactory, cache, jpqlString, startOffset, maxResults);
         }
         if (normalizedHavingClause != ConstantBooleanExpr.TRUE) {
            havingClause = JPATreePrinter.printTree(swapVariables(normalizedHavingClause, parsingResult, columns));
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
            firstPhaseQuery.append("_gen0.").append(c.asStringPath());
         }
      }
      firstPhaseQuery.append(" FROM ").append(parsingResult.getTargetEntityName()).append(" _gen0");
      if (parsingResult.getWhereClause() != null) {
         BooleanExpr normalizedWhereClause = booleanFilterNormalizer.normalize(parsingResult.getWhereClause());
         if (normalizedWhereClause == ConstantBooleanExpr.FALSE) {
            return new EmptyResultQuery(queryFactory, cache, jpqlString, startOffset, maxResults);
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
      if (!parsingResult.getSortFields().isEmpty()) {
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
      Query baseQuery = buildQueryNoAggregations(queryFactory, firstPhaseQueryStr, -1, -1, parse(firstPhaseQueryStr));

      // second phase: grouping, aggregation, 'having' clause filtering, sorting and paging
      String secondPhaseQueryStr = secondPhaseQuery.toString();
      return new AggregatingQuery(queryFactory, cache, secondPhaseQueryStr, _groupFieldPositions, _accumulators,
                                  getObjectFilter(secondPhaseQueryStr, new RowMatcher(_columns)),
                                  startOffset, maxResults, baseQuery);
   }

   private BooleanExpr swapVariables(BooleanExpr expr, final FilterParsingResult<?> parsingResult, final LinkedHashMap<PropertyPath, RowPropertyHelper.ColumnMetadata> columns) {
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
               int idx = columns.size();
               c = new RowPropertyHelper.ColumnMetadata(idx, "C" + idx, propertyHelper.getPrimitivePropertyType(parsingResult.getTargetEntityName(), aggregationExpr.getPropertyPath()));
               columns.put(p, c);
            }
            return new PropertyValueExpr(c.getColumnName(), aggregationExpr.isRepeated());
         }
      }
      return expr.acceptVisitor(new PropertyReplacer());
   }

   private Query buildQueryNoAggregations(QueryFactory queryFactory, String jpqlString, long startOffset, int maxResults, FilterParsingResult<?> parsingResult) {
      if (parsingResult.hasGroupingOrAggregations()) {
         throw new IllegalArgumentException("The query must not use grouping or aggregation"); // may happen only due to programming error
      }

      BooleanExpr normalizedWhereClause = booleanFilterNormalizer.normalize(parsingResult.getWhereClause());
      if (normalizedWhereClause == ConstantBooleanExpr.FALSE) {
         // the query is a contradiction, there are no matches
         return new EmptyResultQuery(queryFactory, cache, jpqlString, startOffset, maxResults);
      }
      if (normalizedWhereClause == null || normalizedWhereClause == ConstantBooleanExpr.TRUE || searchManager == null) {
         return new EmbeddedQuery(queryFactory, cache, makeFilter(jpqlString), startOffset, maxResults);
      }

      BooleShannonExpansion bse = new BooleShannonExpansion(getIndexedFieldProvider(parsingResult));
      BooleanExpr expansion = bse.expand(normalizedWhereClause);

      if (expansion == normalizedWhereClause) {  // identity comparison is intended here!
         // everything is indexed, so go the Lucene way
         //todo [anistor] we should be able to generate the lucene query ourselves rather than depend on hibernate-hql-lucene to do it
         return buildLuceneQuery(queryFactory, jpqlString, startOffset, maxResults);
      }

      if (expansion == ConstantBooleanExpr.TRUE) {
         // expansion leads to a full non-indexed query
         return new EmbeddedQuery(queryFactory, cache, makeFilter(jpqlString), startOffset, maxResults);
      }

      // some fields are indexed, run a hybrid query
      String expandedJpaOut = JPATreePrinter.printTree(parsingResult.getTargetEntityName(), expansion, null);
      Query expandedQuery = buildLuceneQuery(queryFactory, expandedJpaOut, -1, -1);
      return new HybridQuery(queryFactory, cache, jpqlString, getObjectFilter(jpqlString, getSecondPhaseMatcher()), startOffset, maxResults, expandedQuery);
   }

   protected BaseMatcher getFirstPhaseMatcher() {
      return SecurityActions.getCacheComponentRegistry(cache).getComponent(ReflectionMatcher.class);
   }

   protected BaseMatcher getSecondPhaseMatcher() {
      return getFirstPhaseMatcher();
   }

   private FilterParsingResult<?> parse(String jpqlString) {
      FilterParsingResult<?> parsingResult;
      if (queryCache != null) {
         KeyValuePair<String, Class> queryCacheKey = new KeyValuePair<String, Class>(jpqlString, FilterParsingResult.class);
         parsingResult = queryCache.get(queryCacheKey);
         if (parsingResult == null) {
            parsingResult = getFirstPhaseMatcher().parse(jpqlString, null);
            queryCache.put(queryCacheKey, parsingResult);
         }
      } else {
         parsingResult = getFirstPhaseMatcher().parse(jpqlString, null);
      }
      return parsingResult;
   }

   private ObjectFilter getObjectFilter(String jpqlString, BaseMatcher matcher) {
      ObjectFilter objectFilter;
      if (queryCache != null) {
         KeyValuePair<String, Class> queryCacheKey = new KeyValuePair<String, Class>(jpqlString, matcher.getClass());
         objectFilter = queryCache.get(queryCacheKey);
         if (objectFilter == null) {
            objectFilter = matcher.getObjectFilter(jpqlString);
            queryCache.put(queryCacheKey, objectFilter);
         }
      } else {
         objectFilter = matcher.getObjectFilter(jpqlString);
      }
      return objectFilter;
   }

   protected BooleShannonExpansion.IndexedFieldProvider getIndexedFieldProvider(FilterParsingResult<?> parsingResult) {
      return new HibernateSearchIndexedFieldProvider(searchFactory, (Class<?>) parsingResult.getTargetEntityMetadata());
   }

   protected JPAFilterAndConverter makeFilter(final String jpaQuery) {
      //todo [anistor] possible optimisation: if jpaQuery is a tautology and there are no projections just return null, ie. no filtering needed, just get everything
      return SecurityActions.doPrivileged(new PrivilegedAction<JPAFilterAndConverter>() {
         @Override
         public JPAFilterAndConverter run() {
            JPAFilterAndConverter filter = new JPAFilterAndConverter(jpaQuery, ReflectionMatcher.class);
            filter.injectDependencies(cache);

            // force early validation!
            filter.getObjectFilter();
            return filter;
         }
      });
   }

   /**
    * Build a Lucene index query.
    */
   //todo [anistor] make this private when LuceneQuery/LuceneQueryBuilder are removed
   public LuceneQuery buildLuceneQuery(QueryFactory queryFactory, String jpqlString, long startOffset, int maxResults) {
      if (searchManager == null) {
         throw new IllegalStateException("Cannot run Lucene queries on a cache that does not have indexing enabled");
      }

      LuceneQueryParsingResult parsingResult = transformJpaToLucene(jpqlString);
      org.apache.lucene.search.Query luceneQuery = makeTypeQuery(parsingResult.getQuery(), parsingResult.getTargetEntityName());
      CacheQuery cacheQuery = searchManager.getQuery(luceneQuery, parsingResult.getTargetEntity());

      if (parsingResult.getSort() != null) {
         cacheQuery = cacheQuery.sort(parsingResult.getSort());
      }

      String[] projection = null;
      if (parsingResult.getProjections() != null && !parsingResult.getProjections().isEmpty()) {
         int projSize = parsingResult.getProjections().size();
         projection = parsingResult.getProjections().toArray(new String[projSize]);
         cacheQuery = cacheQuery.projection(projection);
      }
      if (startOffset >= 0) {
         cacheQuery = cacheQuery.firstResult((int) startOffset);
      }
      if (maxResults > 0) {
         cacheQuery = cacheQuery.maxResults(maxResults);
      }

      return new EmbeddedLuceneQuery(queryFactory, jpqlString, projection, cacheQuery);
   }

   protected org.apache.lucene.search.Query makeTypeQuery(org.apache.lucene.search.Query query, String targetEntityName) {
      return query;
   }

   private LuceneQueryParsingResult transformJpaToLucene(String jpqlString) {
      LuceneQueryParsingResult parsingResult;
      if (queryCache != null) {
         KeyValuePair<String, Class> queryCacheKey = new KeyValuePair<String, Class>(jpqlString, LuceneQueryParsingResult.class);
         parsingResult = queryCache.get(queryCacheKey);
         if (parsingResult == null) {
            parsingResult = queryParser.parseQuery(jpqlString, makeProcessingChain());
            queryCache.put(queryCacheKey, parsingResult);
         }
      } else {
         parsingResult = queryParser.parseQuery(jpqlString, makeProcessingChain());
      }
      return parsingResult;
   }

   protected LuceneProcessingChain makeProcessingChain() {
      EntityNamesResolver entityNamesResolver = new EntityNamesResolver() {
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
      return new LuceneProcessingChain.Builder(searchFactory, entityNamesResolver).buildProcessingChainForClassBasedEntities(fieldBridgeProvider);
   }
}
