package org.infinispan.query.dsl.impl;

import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.hibernate.hql.QueryParser;
import org.hibernate.hql.ast.spi.EntityNamesResolver;
import org.hibernate.hql.lucene.LuceneProcessingChain;
import org.hibernate.hql.lucene.LuceneQueryParsingResult;
import org.hibernate.search.spi.SearchFactoryIntegrator;
import org.infinispan.query.SearchManager;
import org.infinispan.query.dsl.FilterConditionBeginContext;
import org.infinispan.query.dsl.FilterConditionContext;
import org.infinispan.query.dsl.FilterConditionEndContext;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryBuilder;
import org.infinispan.query.dsl.SortOrder;
import org.infinispan.query.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
class LuceneQueryBuilder implements QueryBuilder, Visitable {

   private static final Log log = LogFactory.getLog(LuceneQueryBuilder.class, Log.class);

   private final SearchManager searchManager;

   private final EntityNamesResolver entityNamesResolver;

   private final Class rootType;

   private String[] projection;

   private BaseCondition filterCondition;

   private List<SortCriteria> sortCriteria;

   private long startOffset = -1;

   private int maxResults = -1;

   public LuceneQueryBuilder(SearchManager searchManager, EntityNamesResolver entityNamesResolver, Class rootType) {
      this.searchManager = searchManager;
      this.entityNamesResolver = entityNamesResolver;
      this.rootType = rootType;
   }

   @Override
   public QueryBuilder orderBy(String attributePath, SortOrder sortOrder) {
      if (sortCriteria == null) {
         sortCriteria = new ArrayList<SortCriteria>();
      }
      sortCriteria.add(new SortCriteria(attributePath, sortOrder));
      return this;
   }

   List<SortCriteria> getSortCriteria() {
      return sortCriteria;
   }

   @Override
   public QueryBuilder setProjection(String... projection) {
      this.projection = projection;
      return this;
   }

   String[] getProjection() {
      return projection;
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

   public Class getRootType() {
      return rootType;
   }

   BaseCondition getFilterCondition() {
      return filterCondition;
   }

   @Override
   public Query build() {
      String jpqlString = accept(new JPAQueryGeneratorVisitor());
      log.tracef("JPQL string : %s", jpqlString);

      Sort sort = null;
      if (sortCriteria != null && !sortCriteria.isEmpty()) {
         SortField[] sortField = new SortField[sortCriteria.size()];
         int i = 0;
         for (SortCriteria sc : sortCriteria) {
            //TODO sort type is hardcoded to String for now
            sortField[i++] = new SortField(sc.getAttributePath(), SortField.STRING, sc.getSortOrder() == SortOrder.DESC);
         }
         sort = new Sort(sortField);
      }

      LuceneProcessingChain processingChain = new LuceneProcessingChain((SearchFactoryIntegrator) searchManager.getSearchFactory(), entityNamesResolver, null);
      QueryParser queryParser = new QueryParser();
      LuceneQueryParsingResult parsingResult = queryParser.parseQuery(jpqlString, processingChain);
      return new LuceneQuery(searchManager, parsingResult, sort, startOffset, maxResults);
   }

   @Override
   public FilterConditionEndContext having(String attributePath) {
      if (filterCondition != null) {
         throw new IllegalStateException("Sentence already started. Cannot use 'having(..)' again.");
      }
      AttributeCondition attributeCondition = new AttributeCondition(attributePath);
      attributeCondition.setQueryBuilder(this);
      filterCondition = attributeCondition;
      return attributeCondition;
   }

   @Override
   public FilterConditionBeginContext not() {
      if (filterCondition != null) {
         throw new IllegalStateException("Sentence already started. Cannot use 'not()' again.");
      }
      IncompleteCondition incompleteCondition = new IncompleteCondition();
      incompleteCondition.setQueryBuilder(this);
      filterCondition = incompleteCondition;
      return incompleteCondition.not();
   }

   @Override
   public FilterConditionContext not(FilterConditionContext fcc) {
      if (filterCondition != null) {
         throw new IllegalStateException("Sentence already started. Cannot use 'not(..)' again.");
      }

      NotCondition notCondition = new NotCondition((BaseCondition) fcc);
      notCondition.setQueryBuilder(this);
      filterCondition = notCondition;
      return filterCondition;
   }

   @Override
   public <ReturnType> ReturnType accept(Visitor<ReturnType> visitor) {
      return visitor.visit(this);
   }
}
