package org.infinispan.query.dsl.impl;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.query.dsl.Expression;
import org.infinispan.query.dsl.FilterConditionContext;
import org.infinispan.query.dsl.FilterConditionEndContext;
import org.infinispan.query.dsl.QueryBuilder;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.SortOrder;
import org.infinispan.query.dsl.impl.logging.Log;
import org.jboss.logging.Logger;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
public abstract class BaseQueryBuilder implements QueryBuilder, Visitable {

   private static final Log log = Logger.getMessageLogger(Log.class, BaseQueryBuilder.class.getName());

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
         throw log.argumentCannotBeNull("rootTypeName");
      }
      this.queryFactory = queryFactory;
      this.rootTypeName = rootTypeName;
   }

   protected String getRootTypeName() {
      return rootTypeName;
   }

   @Override
   public QueryBuilder orderBy(Expression pathExpression) {
      return orderBy(pathExpression, SortOrder.ASC);
   }

   @Override
   public QueryBuilder orderBy(Expression pathExpression, SortOrder sortOrder) {
      if (sortCriteria == null) {
         sortCriteria = new ArrayList<>();
      }
      sortCriteria.add(new SortCriteria(pathExpression, sortOrder));
      return this;
   }

   @Override
   public QueryBuilder orderBy(String attributePath) {
      return orderBy(attributePath, SortOrder.ASC);
   }

   @Override
   public QueryBuilder orderBy(String attributePath, SortOrder sortOrder) {
      return orderBy(Expression.property(attributePath), sortOrder);
   }

   protected List<SortCriteria> getSortCriteria() {
      return sortCriteria;
   }

   @Override
   public QueryBuilder select(String... attributePath) {
      if (attributePath == null || attributePath.length == 0) {
         throw log.projectionCannotBeNullOrEmpty();
      }
      Expression[] projection = new Expression[attributePath.length];
      for (int i = 0; i < attributePath.length; i++) {
         projection[i] = Expression.property(attributePath[i]);
      }
      return select(projection);
   }

   @Override
   public QueryBuilder select(Expression... projection) {
      if (projection == null || projection.length == 0) {
         throw log.projectionCannotBeNullOrEmpty();
      }
      if (this.projection != null) {
         throw log.projectionCanBeSpecifiedOnlyOnce();
      }
      this.projection = projection;
      return this;
   }

   protected Expression[] getProjection() {
      return projection;
   }

   protected String[] getProjectionPaths() {
      if (projection == null) {
         return null;
      }
      String[] _projection = new String[projection.length];
      for (int i = 0; i < projection.length; i++) {
         _projection[i] = projection[i].toString();
      }
      return _projection;
   }

   @Override
   public QueryBuilder groupBy(String... groupBy) {
      if (groupBy == null || groupBy.length == 0) {
         throw log.groupingCannotBeNullOrEmpty();
      }
      if (this.groupBy != null) {
         throw log.groupingCanBeSpecifiedOnlyOnce();
      }
      this.groupBy = groupBy;
      // reset this so we can start a new filter for havingFilterCondition
      filterCondition = null;
      return this;
   }

   protected String[] getGroupBy() {
      return groupBy;
   }

   @Override
   public QueryBuilder startOffset(long startOffset) {
      if (startOffset < 0) {
         throw log.startOffsetCannotBeLessThanZero();
      }
      this.startOffset = startOffset;
      return this;
   }

   @Override
   public QueryBuilder maxResults(int maxResults) {
      if (maxResults <= 0) {
         throw log.maxResultMustBeGreaterThanZero();
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
         throw log.cannotUseOperatorAgain("having(..)");
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
   public BaseCondition not() {
      if (filterCondition != null) {
         throw log.cannotUseOperatorAgain("not()");
      }
      IncompleteCondition incompleteCondition = new IncompleteCondition(queryFactory);
      incompleteCondition.setQueryBuilder(this);
      setFilterCondition(incompleteCondition);
      return incompleteCondition.not();
   }

   @Override
   public BaseCondition not(FilterConditionContext fcc) {
      if (fcc == null) {
         throw log.argumentCannotBeNull();
      }

      if (filterCondition != null) {
         throw log.cannotUseOperatorAgain("not(..)");
      }

      BaseCondition baseCondition = ((BaseCondition) fcc).getRoot();
      if (baseCondition.queryFactory != queryFactory) {
         throw log.conditionWasCreatedByAnotherFactory();
      }
      if (baseCondition.queryBuilder != null) {
         throw log.conditionIsAlreadyInUseByAnotherBuilder();
      }

      NotCondition notCondition = new NotCondition(queryFactory, baseCondition);
      notCondition.setQueryBuilder(this);
      baseCondition.setParent(notCondition);
      setFilterCondition(notCondition);
      return filterCondition;
   }

   @Override
   public <ReturnType> ReturnType accept(Visitor<ReturnType> visitor) {
      return visitor.visit(this);
   }
}
