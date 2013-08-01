package org.infinispan.query.dsl.impl;

import org.hibernate.hql.QueryParser;
import org.hibernate.hql.ast.spi.EntityNamesResolver;
import org.hibernate.hql.lucene.LuceneProcessingChain;
import org.hibernate.hql.lucene.LuceneQueryParsingResult;
import org.hibernate.search.spi.SearchFactoryIntegrator;
import org.infinispan.query.SearchManager;
import org.infinispan.query.dsl.FilterConditionBeginContext;
import org.infinispan.query.dsl.FilterConditionEndContext;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryBuilder;
import org.infinispan.query.dsl.SortOrder;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
class LuceneQueryBuilder implements QueryBuilder, Visitable {

   private final SearchManager searchManager;

   private final EntityNamesResolver entityNamesResolver;

   private final Class rootType;

   private AttributeCondition filterCondition;

   private long startOffset = -1;

   private int maxResults = -1;

   public LuceneQueryBuilder(SearchManager searchManager, EntityNamesResolver entityNamesResolver, Class rootType) {
      this.searchManager = searchManager;
      this.entityNamesResolver = entityNamesResolver;
      this.rootType = rootType;
   }

   @Override
   public QueryBuilder orderBy(String attributePath, SortOrder sortOrder) {
      return this;  // TODO: Customise this generated block
   }

   @Override
   public QueryBuilder setProjection(String... attributePath) {
      return this;  // TODO: Customise this generated block
   }

   @Override
   public QueryBuilder startOffset(long startOffset) {
      this.startOffset = startOffset;
      return this;
   }

   @Override
   public QueryBuilder maxResults(int maxResults) {
      this.maxResults = maxResults;
      return this;
   }

   @Override
   public Class getRootType() {
      return rootType;
   }

   AttributeCondition getFilterCondition() {
      return filterCondition;
   }

   @Override
   public Query build() {
      String jpaQuery = accept(new JPAQueryGeneratorVisitor());
      LuceneProcessingChain processingChain = new LuceneProcessingChain((SearchFactoryIntegrator) searchManager.getSearchFactory(), entityNamesResolver, null);
      QueryParser queryParser = new QueryParser();
      LuceneQueryParsingResult parsingResult = queryParser.parseQuery(jpaQuery, processingChain);
      return new LuceneQuery(searchManager, parsingResult);
   }

   @Override
   public FilterConditionEndContext having(String attributePath) {
      if (filterCondition != null) {
         throw new IllegalStateException("Sentence already started. Cannot use 'having(..)' again.");
      }
      filterCondition = new AttributeCondition();
      filterCondition.setQueryBuilder(this);
      return filterCondition.having(attributePath);
   }

   @Override
   public FilterConditionBeginContext not() {
      if (filterCondition != null) {
         throw new IllegalStateException("Sentence already started. Cannot use 'not()' again.");
      }
      filterCondition = new AttributeCondition();
      filterCondition.setQueryBuilder(this);
      return filterCondition.not();
   }

   @Override
   public <ReturnType> ReturnType accept(Visitor<ReturnType> visitor) {
      return visitor.visit(this);
   }
}
