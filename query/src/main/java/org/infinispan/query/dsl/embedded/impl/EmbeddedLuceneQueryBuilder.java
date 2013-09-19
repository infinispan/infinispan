package org.infinispan.query.dsl.embedded.impl;

import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.hibernate.hql.QueryParser;
import org.hibernate.hql.ast.spi.EntityNamesResolver;
import org.hibernate.hql.lucene.LuceneProcessingChain;
import org.hibernate.hql.lucene.LuceneQueryParsingResult;
import org.hibernate.search.engine.metadata.impl.DocumentFieldMetadata;
import org.hibernate.search.engine.metadata.impl.PropertyMetadata;
import org.hibernate.search.engine.spi.EntityIndexBinding;
import org.hibernate.search.engine.spi.SearchFactoryImplementor;
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

      SearchFactoryIntegrator searchFactory = (SearchFactoryIntegrator) searchManager.getSearchFactory();
      LuceneProcessingChain processingChain = new LuceneProcessingChain.Builder(searchFactory, entityNamesResolver).buildProcessingChainForClassBasedEntities();
      QueryParser queryParser = new QueryParser();
      LuceneQueryParsingResult parsingResult = queryParser.parseQuery(jpqlString, processingChain);

      Sort sort = null;
      if (sortCriteria != null && !sortCriteria.isEmpty()) {
         SortField[] sortField = new SortField[sortCriteria.size()];
         int i = 0;
         for (SortCriteria sc : sortCriteria) {
            //TODO [anistor] sort type is not entirely correct
            PropertyMetadata propMetadata = getPropertyMetadata(parsingResult.getTargetEntity(), sc.getAttributePath());
            DocumentFieldMetadata fm = propMetadata.getFieldMetadata().iterator().next();
            int sortType = fm.isNumeric() ? SortField.INT : SortField.STRING;
            sortField[i++] = new SortField(sc.getAttributePath(), sortType, sc.getSortOrder() == SortOrder.DESC);
         }
         sort = new Sort(sortField);
      }

      return new EmbeddedLuceneQuery(searchManager, parsingResult, sort, startOffset, maxResults);
   }

   private PropertyMetadata getPropertyMetadata(Class<?> type, String propName) {
      EntityIndexBinding entityIndexBinding = ((SearchFactoryImplementor) searchManager.getSearchFactory()).getIndexBinding(type);
      if (entityIndexBinding == null) {
         throw new IllegalArgumentException("The type " + type.getName() + " is not an indexed entity.");
      }

      return entityIndexBinding.getDocumentBuilder().getMetadata().getPropertyMetadataForProperty(propName);
   }
}
