package org.infinispan.query.dsl.impl;

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
   protected String[] projection;

   protected BaseCondition filterCondition;

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
   public QueryBuilder<T> orderBy(String attributePath, SortOrder sortOrder) {
      if (sortCriteria == null) {
         sortCriteria = new ArrayList<SortCriteria>();
      }
      sortCriteria.add(new SortCriteria(attributePath, sortOrder));
      return this;
   }

   protected List<SortCriteria> getSortCriteria() {
      return sortCriteria;
   }

   @Override
   public QueryBuilder<T> setProjection(String... projection) {
      this.projection = projection;
      return this;
   }

   protected String[] getProjection() {
      return projection;
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

   protected BaseCondition getFilterCondition() {
      return filterCondition;
   }

   @Override
   public FilterConditionEndContext having(String attributePath) {
      if (filterCondition != null) {
         throw new IllegalStateException("Sentence already started. Cannot use 'having(..)' again.");
      }
      AttributeCondition attributeCondition = new AttributeCondition(queryFactory, attributePath);
      attributeCondition.setQueryBuilder(this);
      filterCondition = attributeCondition;
      return attributeCondition;
   }

   @Override
   public FilterConditionBeginContext not() {
      if (filterCondition != null) {
         throw new IllegalStateException("Sentence already started. Cannot use 'not()' again.");
      }
      IncompleteCondition incompleteCondition = new IncompleteCondition(queryFactory);
      incompleteCondition.setQueryBuilder(this);
      filterCondition = incompleteCondition;
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
      filterCondition = notCondition;
      return filterCondition;
   }

   @Override
   public <ReturnType> ReturnType accept(Visitor<ReturnType> visitor) {
      return visitor.visit(this);
   }
}
