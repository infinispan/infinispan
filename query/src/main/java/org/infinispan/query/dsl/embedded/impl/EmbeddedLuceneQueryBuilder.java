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
import org.infinispan.util.logging.LogFactory;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
final class EmbeddedLuceneQueryBuilder extends BaseQueryBuilder<LuceneQuery> {

   private static final Log log = LogFactory.getLog(EmbeddedLuceneQueryBuilder.class, Log.class);

   private final SearchManager searchManager;

   private final EntityNamesResolver entityNamesResolver;

   public EmbeddedLuceneQueryBuilder(EmbeddedLuceneQueryFactory queryFactory, SearchManager searchManager, EntityNamesResolver entityNamesResolver, String rootType) {
      super(queryFactory, rootType);
      this.searchManager = searchManager;
      this.entityNamesResolver = entityNamesResolver;
   }

   @Override
   public LuceneQuery build() {
      String jpqlString = accept(new JPAQueryGenerator());
      if (log.isTraceEnabled()) {
         log.tracef("JPQL string : %s", jpqlString);
      }

      SearchFactoryIntegrator searchFactory = (SearchFactoryIntegrator) searchManager.getSearchFactory();
      LuceneProcessingChain processingChain = new LuceneProcessingChain.Builder(searchFactory, entityNamesResolver).buildProcessingChainForClassBasedEntities();
      QueryParser queryParser = new QueryParser();
      LuceneQueryParsingResult parsingResult = queryParser.parseQuery(jpqlString, processingChain);

      return new EmbeddedLuceneQuery(searchManager, parsingResult, startOffset, maxResults);
   }
}
