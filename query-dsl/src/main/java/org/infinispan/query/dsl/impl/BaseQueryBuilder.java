package org.infinispan.query.dsl.impl;

import org.infinispan.query.dsl.Expression;
import org.infinispan.query.dsl.FilterConditionBeginContext;
import org.infinispan.query.dsl.FilterConditionContext;
import org.infinispan.query.dsl.FilterConditionEndContext;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryBuilder;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.SortOrder;

import java.util.ArrayList;
import java.util.List;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
public abstract class BaseQueryBuilder<T extends Query> implements QueryBuilder<T>, Visitable {

   protected final QueryFactory queryFactory;

   /**
    * The fully qualified name of the entity being queried. It can be a Java Class name or a Protobuf message type
    * name.
    */
   protected final String rootTypeName;

   /**
    * The attribute paths for the projection.
    */
   protected Expression[] projection;

   protected String[] groupBy;

   protected BaseCondition filterCondition;

   protected BaseCondition whereFilterCondition;

   protected BaseCondition havingFilterCondition;

   protected List<SortCriteria> sortCriteria;

   protected long startOffset = -1;

   protected int maxResults = -1;

   protected BaseQueryBuilder(QueryFactory queryFactory, String rootTypeName) {
      if (rootTypeName == null) {
         throw new IllegalArgumentException("rootTypeName cannot be null");
      }
      this.queryFactory = queryFactory;
      this.rootTypeName = rootTypeName;
   }

   protected String getRootTypeName() {
      return rootTypeName;
   }

   @Override
   public QueryBuilder<T> orderBy(Expression pathExpression) {
      return orderBy(pathExpression, SortOrder.ASC);
   }

   @Override
   public QueryBuilder<T> orderBy(Expression pathExpression, SortOrder sortOrder) {
      if (sortCriteria == null) {
         sortCriteria = new ArrayList<SortCriteria>();
      }
      sortCriteria.add(new SortCriteria(pathExpression, sortOrder));
      return this;
   }

   @Override
   public QueryBuilder<T> orderBy(String attributePath) {
      return orderBy(attributePath, SortOrder.ASC);
   }

   @Override
   public QueryBuilder<T> orderBy(String attributePath, SortOrder sortOrder) {
      return orderBy(Expression.property(attributePath), sortOrder);
   }

   protected List<SortCriteria> getSortCriteria() {
      return sortCriteria;
   }

   @Override
   public QueryBuilder<T> select(String... attributePath) {
      if (attributePath == null || attributePath.length == 0) {
         throw new IllegalArgumentException("Projection cannot be null or empty");
      }
      Expression[] projection = new Expression[attributePath.length];
      for (int i = 0 ; i < attributePath.length; i++) {
         projection[i] = Expression.property(attributePath[i]);
      }
      return select(projection);
   }

   @Override
   public QueryBuilder<T> select(Expression... projection) {
      if (projection == null || projection.length == 0) {
         throw new IllegalArgumentException("Projection cannot be null or empty");
      }
      if (this.projection != null) {
         throw new IllegalStateException("Projection can be specified only once");
      }
      this.projection = projection;
      return this;
   }

   @Override
   @Deprecated
   public QueryBuilder<T> setProjection(String... projection) {
      return select(projection);
   }

   protected Expression[] getProjection() {
      return projection;
   }

   @Override
   public QueryBuilder groupBy(String... groupBy) {
      if (groupBy == null || groupBy.length == 0) {
         throw new IllegalArgumentException("Grouping cannot be null or empty");
      }
      if (this.groupBy != null) {
         throw new IllegalStateException("Grouping can be specified only once");
      }
      this.groupBy = groupBy;
      return this;
   }

   protected String[] getGroupBy() {
      return groupBy;
   }

   @Override
   public QueryBuilder<T> startOffset(long startOffset) {
      if (startOffset < 0) {
         throw new IllegalArgumentException("startOffset cannot be less than 0");
      }
      this.startOffset = startOffset;
      return this;
   }

   @Override
   public QueryBuilder<T> maxResults(int maxResults) {
      if (maxResults <= 0) {
         throw new IllegalArgumentException("maxResults must be greater than 0");
      }
      this.maxResults = maxResults;
      return this;
   }

   protected BaseCondition getWhereFilterCondition() {
      return whereFilterCondition;
   }

   protected BaseCondition getHavingFilterCondition() {
      return havingFilterCondition;
   }

   @Override
   public FilterConditionEndContext having(Expression expression) {
      if (filterCondition != null) {
         throw new IllegalStateException("Sentence already started. Cannot use 'having(..)' again.");
      }
      AttributeCondition attributeCondition = new AttributeCondition(queryFactory, expression);
      attributeCondition.setQueryBuilder(this);
      setFilterCondition(attributeCondition);
      return attributeCondition;
   }

   @Override
   public FilterConditionEndContext having(String attributePath) {
      return having(Expression.property(attributePath));
   }

   private void setFilterCondition(BaseCondition filterCondition) {
      this.filterCondition = filterCondition;
      if (groupBy == null) {
         whereFilterCondition = filterCondition;
      } else {
         havingFilterCondition = filterCondition;
      }
   }

   @Override
   public FilterConditionBeginContext not() {
      if (filterCondition != null) {
         throw new IllegalStateException("Sentence already started. Cannot use 'not()' again.");
      }
      IncompleteCondition incompleteCondition = new IncompleteCondition(queryFactory);
      incompleteCondition.setQueryBuilder(this);
      setFilterCondition(incompleteCondition);
      return incompleteCondition.not();
   }

   @Override
   public FilterConditionContext not(FilterConditionContext fcc) {
      if (fcc == null) {
         throw new IllegalArgumentException("Argument cannot be null");
      }

      if (filterCondition != null) {
         throw new IllegalStateException("Sentence already started. Cannot use 'not(..)' again.");
      }

      BaseCondition baseCondition = ((BaseCondition) fcc).getRoot();
      if (baseCondition.queryFactory != queryFactory) {
         throw new IllegalArgumentException("The given condition was created by a different factory");
      }
      if (baseCondition.queryBuilder != null) {
         throw new IllegalArgumentException("The given condition is already in use by another builder");
      }

      NotCondition notCondition = new NotCondition(queryFactory, baseCondition);
      notCondition.setQueryBuilder(this);
      setFilterCondition(notCondition);
      return filterCondition;
   }

   @Override
   public <ReturnType> ReturnType accept(Visitor<ReturnType> visitor) {
      return visitor.visit(this);
   }
}
