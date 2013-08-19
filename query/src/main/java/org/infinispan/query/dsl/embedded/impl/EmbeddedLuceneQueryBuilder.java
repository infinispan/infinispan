package org.infinispan.query.dsl.embedded.impl;

import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.hibernate.hql.QueryParser;
import org.hibernate.hql.ast.spi.EntityNamesResolver;
import org.hibernate.hql.lucene.LuceneProcessingChain;
import org.hibernate.hql.lucene.LuceneQueryParsingResult;
import org.hibernate.search.spi.SearchFactoryIntegrator;
import org.infinispan.query.SearchManager;
import org.infinispan.query.dsl.SortOrder;
import org.infinispan.query.dsl.embedded.LuceneQuery;
import org.infinispan.query.dsl.impl.BaseQueryBuilder;
import org.infinispan.query.dsl.impl.JPAQueryGenerator;
import org.infinispan.query.dsl.impl.SortCriteria;
import org.infinispan.query.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
class EmbeddedLuceneQueryBuilder extends BaseQueryBuilder<LuceneQuery> {

   private static final Log log = LogFactory.getLog(EmbeddedLuceneQueryBuilder.class, Log.class);

   private final SearchManager searchManager;

   private final EntityNamesResolver entityNamesResolver;

   public EmbeddedLuceneQueryBuilder(SearchManager searchManager, EntityNamesResolver entityNamesResolver, Class rootType) {
      super(rootType);
      this.searchManager = searchManager;
      this.entityNamesResolver = entityNamesResolver;
   }

   @Override
   public LuceneQuery build() {
      String jpqlString = accept(new JPAQueryGenerator());
      if (log.isTraceEnabled()) {
         log.tracef("JPQL string : %s", jpqlString);
      }

      Sort sort = null;
      if (sortCriteria != null && !sortCriteria.isEmpty()) {
         SortField[] sortField = new SortField[sortCriteria.size()];
         int i = 0;
         for (SortCriteria sc : sortCriteria) {
            //TODO [anistor] sort type is hardcoded to String for now
            sortField[i++] = new SortField(sc.getAttributePath(), SortField.STRING, sc.getSortOrder() == SortOrder.DESC);
         }
         sort = new Sort(sortField);
      }

      LuceneProcessingChain processingChain = new LuceneProcessingChain((SearchFactoryIntegrator) searchManager.getSearchFactory(), entityNamesResolver, null);
      QueryParser queryParser = new QueryParser();
      LuceneQueryParsingResult parsingResult = queryParser.parseQuery(jpqlString, processingChain);
      return new EmbeddedLuceneQuery(searchManager, parsingResult, sort, startOffset, maxResults);
   }
}
