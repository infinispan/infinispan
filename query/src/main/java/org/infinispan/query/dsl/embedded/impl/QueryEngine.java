package org.infinispan.query.dsl.embedded.impl;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
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
import org.infinispan.objectfilter.Matcher;
import org.infinispan.objectfilter.ObjectFilter;
import org.infinispan.objectfilter.impl.BaseMatcher;
import org.infinispan.objectfilter.impl.ReflectionMatcher;
import org.infinispan.objectfilter.impl.hql.FilterParsingResult;
import org.infinispan.objectfilter.impl.syntax.BooleShannonExpansion;
import org.infinispan.objectfilter.impl.syntax.BooleanExpr;
import org.infinispan.objectfilter.impl.syntax.BooleanFilterNormalizer;
import org.infinispan.objectfilter.impl.syntax.ConstantBooleanExpr;
import org.infinispan.objectfilter.impl.syntax.JPATreePrinter;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.SearchManager;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.embedded.LuceneQuery;
import org.infinispan.query.impl.ComponentRegistryUtils;
import org.infinispan.util.KeyValuePair;

import java.security.PrivilegedAction;
import java.util.Arrays;

/**
 * @author anistor@redhat.com
 * @since 7.2
 */
public class QueryEngine {

   private final AdvancedCache<?, ?> cache;

   /**
    * Optional cache for query objects.
    */
   private final QueryCache queryCache;

   private final SearchManager searchManager;

   private final SearchIntegrator searchFactory;

   private final QueryParser queryParser = new QueryParser();

   private final BooleanFilterNormalizer booleanFilterNormalizer = new BooleanFilterNormalizer();

   private final EntityNamesResolver entityNamesResolver = new EntityNamesResolver() {
      @Override
      public Class<?> getClassFromName(String entityName) {
         try {
            return Util.loadClassStrict(entityName, null);
         } catch (ClassNotFoundException e) {
            return null;
         }
      }
   };

   public QueryEngine(AdvancedCache<?, ?> cache, SearchManager searchManager) {
      this.cache = cache;
      this.queryCache = ComponentRegistryUtils.getQueryCache(cache);
      this.searchManager = searchManager;
      searchFactory = searchManager != null ? searchManager.unwrap(SearchIntegrator.class) : null;
   }

   public Query buildQuery(QueryFactory queryFactory, String jpqlString, long startOffset, int maxResults) {
      Class<? extends Matcher> matcherImplClass = ReflectionMatcher.class;

      if (searchManager != null) {
         BaseMatcher matcher = (BaseMatcher) cache.getAdvancedCache().getComponentRegistry().getComponent(matcherImplClass);
         FilterParsingResult<Class<?>> parsingResult = matcher.parse(jpqlString, null);
         BooleanExpr normalizedExpr = booleanFilterNormalizer.normalize(parsingResult.getQuery());

         if (normalizedExpr == ConstantBooleanExpr.FALSE) {
            return new EmptyResultQuery(queryFactory, cache, jpqlString, startOffset, maxResults);
         }

         BooleShannonExpansion bse = new BooleShannonExpansion(new HibernateSearchIndexedFieldProvider(searchFactory, parsingResult.getTargetEntityMetadata()));
         BooleanExpr expansion = bse.expand(normalizedExpr);

         if (expansion == normalizedExpr) {  // identity comparison is intended here!
            // everything is indexed, so go the Lucene way
            //todo [anistor] we should be able to generate the lucene query ourselves rather than depend on hibernate-hql-lucene to do it
            return buildLuceneQuery(queryFactory, jpqlString, startOffset, maxResults, null);
         }

         if (expansion == ConstantBooleanExpr.TRUE) {
            // expansion leads to a full non-indexed query
            return new EmbeddedQuery(queryFactory, cache, makeFilter(cache, jpqlString, matcherImplClass), startOffset, maxResults);
         }

         String expandedJpaOut = JPATreePrinter.printTree(parsingResult.getTargetEntityName(), expansion, null);
         Query expandedQuery = buildLuceneQuery(queryFactory, expandedJpaOut, -1, -1, null);
         ObjectFilter objectFilter = matcher.getObjectFilter(jpqlString);
         return new HybridQuery(queryFactory, cache, jpqlString, objectFilter, startOffset, maxResults, expandedQuery);
      } else {
         return new EmbeddedQuery(queryFactory, cache, makeFilter(cache, jpqlString, matcherImplClass), startOffset, maxResults);
      }
   }

   private JPAFilterAndConverter makeFilter(final AdvancedCache<?, ?> cache, final String jpaQuery, final Class<? extends Matcher> matcherImplClass) {
      return SecurityActions.doPrivileged(new PrivilegedAction<JPAFilterAndConverter>() {
         @Override
         public JPAFilterAndConverter run() {
            JPAFilterAndConverter filter = new JPAFilterAndConverter(jpaQuery, matcherImplClass);
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
   public LuceneQuery buildLuceneQuery(QueryFactory queryFactory, String jpqlString, long startOffset, int maxResults, org.apache.lucene.search.Query additionalLuceneQuery) {
      if (searchManager == null) {
         throw new IllegalStateException("Cannot run Lucene queries on a cache that does not have indexing enabled");
      }

      LuceneQueryParsingResult parsingResult;

      if (queryCache != null) {
         KeyValuePair<String, Class> queryCacheKey = new KeyValuePair<String, Class>(jpqlString, LuceneQueryParsingResult.class);
         parsingResult = queryCache.get(queryCacheKey);
         if (parsingResult == null) {
            parsingResult = transformJpaToLucene(jpqlString);
            queryCache.put(queryCacheKey, parsingResult);
         }
      } else {
         parsingResult = transformJpaToLucene(jpqlString);
      }

      org.apache.lucene.search.Query luceneQuery = parsingResult.getQuery();

      if (additionalLuceneQuery != null) {
         BooleanQuery booleanQuery = new BooleanQuery();
         booleanQuery.add(new BooleanClause(additionalLuceneQuery, BooleanClause.Occur.MUST));
         booleanQuery.add(new BooleanClause(luceneQuery, BooleanClause.Occur.MUST));
         luceneQuery = booleanQuery;
      }

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

   private LuceneQueryParsingResult transformJpaToLucene(String jpqlString) {
      FieldBridgeProvider fieldBridgeProvider = new FieldBridgeProvider() {

         private final ClassBasedLucenePropertyHelper propertyHelper = new ClassBasedLucenePropertyHelper(searchFactory, entityNamesResolver);

         @Override
         public FieldBridge getFieldBridge(String type, String propertyPath) {
            return propertyHelper.getFieldBridge(type, Arrays.asList(propertyPath.split("[.]")));
         }
      };
      LuceneProcessingChain processingChain = new LuceneProcessingChain.Builder(searchFactory, entityNamesResolver)
            .buildProcessingChainForClassBasedEntities(fieldBridgeProvider);
      return queryParser.parseQuery(jpqlString, processingChain);
   }
}
