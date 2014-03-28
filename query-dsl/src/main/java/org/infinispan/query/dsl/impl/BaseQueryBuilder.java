package org.infinispan.query.dsl.impl;

import org.infinispan.query.dsl.FilterConditionBeginContext;
import org.infinispan.query.dsl.FilterConditionContext;
import org.infinispan.query.dsl.FilterConditionEndContext;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryBuilder;
import org.infinispan.query.dsl.SortOrder;

import java.util.ArrayList;
import java.util.List;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
public abstract class BaseQueryBuilder<T extends Query> implements QueryBuilder<T>, Visitable {

   /**
    * The fully qualified name of the entity being queried. It can be a Java Class name or a Protobuf message type name.
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

   protected BaseQueryBuilder(String rootTypeName) {
      if (rootTypeName == null) {
         throw new IllegalArgumentException("rootTypeName cannot be null");
      }
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
      this.startOffset = startOffset;
      return this;
   }

   @Override
   public QueryBuilder<T> maxResults(int maxResults) {
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
