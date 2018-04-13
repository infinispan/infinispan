package org.infinispan.query.dsl.impl;

import org.infinispan.query.dsl.Expression;
import org.infinispan.query.dsl.FilterConditionBeginContext;
import org.infinispan.query.dsl.FilterConditionContext;
import org.infinispan.query.dsl.FilterConditionContextQueryBuilder;
import org.infinispan.query.dsl.FilterConditionEndContext;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryBuilder;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.SortOrder;
import org.infinispan.query.dsl.impl.logging.Log;
import org.jboss.logging.Logger;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
abstract class BaseCondition implements FilterConditionContextQueryBuilder, Visitable {

   private static final Log log = Logger.getMessageLogger(Log.class, BaseCondition.class.getName());

   protected BaseCondition parent = null;

   protected QueryBuilder queryBuilder;

   protected final QueryFactory queryFactory;

   protected BaseCondition(QueryFactory queryFactory) {
      this.queryFactory = queryFactory;
   }

   @SuppressWarnings("deprecation")
   @Override
   public QueryBuilder toBuilder() {
      return getQueryBuilder();
   }

   QueryBuilder getQueryBuilder() {
      if (queryBuilder == null) {
         throw log.subQueryDoesNotBelongToAParentQueryBuilder();
      }
      return queryBuilder;
   }

   void setQueryBuilder(QueryBuilder queryBuilder) {
      if (this.queryBuilder != null) {
         throw log.queryAlreadyBelongsToAnotherBuilder();
      }
      this.queryBuilder = queryBuilder;
   }

   /**
    * Returns the topmost condition, never {@code null}.
    *
    * @return the topmost condition following up the parent chain or {@code this} if parent is {@code null}.
    */
   public BaseCondition getRoot() {
      BaseCondition p = this;
      while (p.getParent() != null) {
         p = p.getParent();
      }
      return p;
   }

   public BaseCondition getParent() {
      return parent;
   }

   public void setParent(BaseCondition parent) {
      this.parent = parent;
   }

   @Override
   public FilterConditionBeginContext and() {
      IncompleteCondition rightCondition = new IncompleteCondition(queryFactory);
      combine(true, rightCondition);
      return rightCondition;
   }

   @Override
   public FilterConditionContextQueryBuilder and(FilterConditionContext rightCondition) {
      combine(true, rightCondition);
      return this;
   }

   @Override
   public FilterConditionBeginContext or() {
      IncompleteCondition rightCondition = new IncompleteCondition(queryFactory);
      combine(false, rightCondition);
      return rightCondition;
   }

   @Override
   public BaseCondition or(FilterConditionContext rightCondition) {
      combine(false, rightCondition);
      return this;
   }

   private void combine(boolean isConjunction, FilterConditionContext fcc) {
      if (fcc == null) {
         throw log.argumentCannotBeNull();
      }

      BaseCondition rightCondition = ((BaseCondition) fcc).getRoot();
      if (rightCondition.queryFactory != queryFactory) {
         throw log.conditionWasCreatedByAnotherFactory();
      }

      if (rightCondition.queryBuilder != null) {
         throw log.conditionIsAlreadyInUseByAnotherBuilder();
      }

      if (isConjunction && parent instanceof OrCondition) {
         BooleanCondition p = new AndCondition(queryFactory, this, rightCondition);
         ((BooleanCondition) parent).replaceChildCondition(this, p);
         parent = p;
         rightCondition.setParent(p);
      } else {
         BaseCondition root = getRoot();
         BooleanCondition p = isConjunction ? new AndCondition(queryFactory, root, rightCondition) : new OrCondition(queryFactory, root, rightCondition);
         root.setParent(p);
         rightCondition.setParent(p);
      }

      rightCondition.setQueryBuilder(queryBuilder);
   }

   //////////////////////////////// Delegate to parent QueryBuilder ///////////////////////////

   @Override
   public QueryBuilder startOffset(long startOffset) {
      return getQueryBuilder().startOffset(startOffset);
   }

   @Override
   public QueryBuilder maxResults(int maxResults) {
      return getQueryBuilder().maxResults(maxResults);
   }

   @Override
   public QueryBuilder select(Expression... projection) {
      return getQueryBuilder().select(projection);
   }

   @Override
   public QueryBuilder select(String... attributePath) {
      return getQueryBuilder().select(attributePath);
   }

   @Override
   public QueryBuilder groupBy(String... attributePath) {
      return getQueryBuilder().groupBy(attributePath);
   }

   @Override
   public QueryBuilder orderBy(Expression expression) {
      return getQueryBuilder().orderBy(expression);
   }

   @Override
   public QueryBuilder orderBy(Expression expression, SortOrder sortOrder) {
      return getQueryBuilder().orderBy(expression);
   }

   @Override
   public QueryBuilder orderBy(String attributePath) {
      return getQueryBuilder().orderBy(attributePath);
   }

   @Override
   public QueryBuilder orderBy(String attributePath, SortOrder sortOrder) {
      return getQueryBuilder().orderBy(attributePath, sortOrder);
   }

   @Override
   public FilterConditionEndContext having(String attributePath) {
      return getQueryBuilder().having(attributePath);
   }

   @Override
   public FilterConditionEndContext having(Expression expression) {
      return getQueryBuilder().having(expression);
   }

   @Override
   public FilterConditionContextQueryBuilder not() {
      return getQueryBuilder().not();
   }

   @Override
   public FilterConditionContextQueryBuilder not(FilterConditionContext fcc) {
      return getQueryBuilder().not(fcc);
   }

   @Override
   public Query build() {
      return getQueryBuilder().build();
   }
}
