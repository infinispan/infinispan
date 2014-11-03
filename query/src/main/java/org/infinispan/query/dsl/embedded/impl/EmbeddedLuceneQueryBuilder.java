package org.infinispan.query.dsl.embedded.impl;

import org.hibernate.hql.QueryParser;
import org.hibernate.hql.ast.spi.EntityNamesResolver;
import org.hibernate.hql.lucene.LuceneProcessingChain;
import org.hibernate.hql.lucene.LuceneQueryParsingResult;
import org.hibernate.search.spi.SearchFactoryIntegrator;
import org.infinispan.query.SearchManager;
import org.infinispan.query.dsl.embedded.LuceneQuery;
import org.infinispan.query.dsl.impl.BaseQueryBuilder;
import org.infinispan.query.dsl.impl.JPAQueryGenerator;
import org.infinispan.query.logging.Log;
import org.infinispan.util.KeyValuePair;
import org.infinispan.util.logging.LogFactory;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
final class EmbeddedLuceneQueryBuilder extends BaseQueryBuilder<LuceneQuery> {

   private static final Log log = LogFactory.getLog(EmbeddedLuceneQueryBuilder.class, Log.class);

   private final SearchManager searchManager;

   /**
    * Optional cache for query objects.
    */
   private final QueryCache queryCache;

   private final EntityNamesResolver entityNamesResolver;

   public EmbeddedLuceneQueryBuilder(EmbeddedLuceneQueryFactory queryFactory, SearchManager searchManager, QueryCache queryCache, EntityNamesResolver entityNamesResolver, String rootType) {
      super(queryFactory, rootType);
      this.searchManager = searchManager;
      this.queryCache = queryCache;
      this.entityNamesResolver = entityNamesResolver;
   }

   @Override
   public LuceneQuery build() {
      String jpqlString = accept(new JPAQueryGenerator());
      if (log.isTraceEnabled()) {
         log.tracef("JPQL string : %s", jpqlString);
      }

      LuceneQueryParsingResult parsingResult;
      if (queryCache != null) {
         KeyValuePair<String, Class> queryCacheKey = new KeyValuePair<String, Class>(jpqlString, LuceneQueryParsingResult.class);
         parsingResult = queryCache.get(queryCacheKey);
         if (parsingResult == null) {
            parsingResult = parse(jpqlString);
            queryCache.put(queryCacheKey, parsingResult);
         }
      } else {
         parsingResult = parse(jpqlString);
      }

      return new EmbeddedLuceneQuery(searchManager, parsingResult, startOffset, maxResults);
   }

   private LuceneQueryParsingResult parse(String jpqlString) {
      SearchFactoryIntegrator searchFactory = searchManager.getSearchFactory();
      LuceneProcessingChain processingChain = new LuceneProcessingChain.Builder(searchFactory, entityNamesResolver).buildProcessingChainForClassBasedEntities();
      QueryParser queryParser = new QueryParser();
      return queryParser.parseQuery(jpqlString, processingChain);
   }
}
