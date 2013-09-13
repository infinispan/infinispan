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

   protected final Class rootType;

   protected String[] projection;

   protected BaseCondition filterCondition;

   protected List<SortCriteria> sortCriteria;

   protected long startOffset = -1;

   protected int maxResults = -1;

   protected BaseQueryBuilder(Class rootType) {
      if (rootType == null) {
         throw new IllegalArgumentException("rootType cannot be null");
      }
      this.rootType = rootType;
   }

   protected Class getRootType() {
      return rootType;
   }

   @Override
   public QueryBuilder orderBy(String attributePath, SortOrder sortOrder) {
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
   public QueryBuilder setProjection(String... projection) {
      this.projection = projection;
      return this;
   }

   protected String[] getProjection() {
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
