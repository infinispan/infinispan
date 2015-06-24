package org.infinispan.objectfilter.impl.hql.predicate;

import org.hibernate.hql.ast.origin.hql.resolve.path.AggregationPropertyPath;
import org.hibernate.hql.ast.spi.EntityNamesResolver;
import org.hibernate.hql.ast.spi.SingleEntityHavingQueryBuilder;
import org.hibernate.hql.ast.spi.predicate.ComparisonPredicate;
import org.hibernate.hql.ast.spi.predicate.ParentPredicate;
import org.hibernate.hql.ast.spi.predicate.Predicate;
import org.hibernate.hql.ast.spi.predicate.RootPredicate;
import org.infinispan.objectfilter.PropertyPath;
import org.infinispan.objectfilter.impl.hql.ObjectPropertyHelper;
import org.infinispan.objectfilter.impl.syntax.AggregationExpr;
import org.infinispan.objectfilter.impl.syntax.BooleanExpr;
import org.infinispan.objectfilter.impl.syntax.PropertyValueExpr;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

/**
 * Builder for the creation of HAVING clause filters targeting a single entity, based on HQL/JPQL queries.
 * <p/>
 * Implemented as a stack of {@link Predicate}s which allows to add elements to the constructed query in a uniform
 * manner while traversing through the original HQL/JPQL query parse tree.
 *
 * @author anistor@redhat.com
 * @since 8.0
 */
public final class SingleEntityHavingQueryBuilderImpl implements SingleEntityHavingQueryBuilder<BooleanExpr> {

   private final EntityNamesResolver entityNamesResolver;

   private final ObjectPropertyHelper propertyHelper;

   /**
    * The targeted entity type of the built query.
    */
   private String entityType;

   /**
    * The root predicate of the {@code HAVING} clause of the built query.
    */
   private RootPredicate<BooleanExpr> rootPredicate;

   /**
    * Keeps track of all the parent predicates ({@code AND}, {@code OR} etc.) of the {@code HAVING} clause of the built
    * query.
    */
   private final Stack<ParentPredicate<BooleanExpr>> predicates = new Stack<ParentPredicate<BooleanExpr>>();

   public SingleEntityHavingQueryBuilderImpl(EntityNamesResolver entityNamesResolver, ObjectPropertyHelper propertyHelper) {
      this.entityNamesResolver = entityNamesResolver;
      this.propertyHelper = propertyHelper;
   }

   @Override
   public void setEntityType(String entityType) {
      if (entityNamesResolver.getClassFromName(entityType) == null) {
         throw new IllegalStateException("Unknown entity name " + entityType);
      }
      this.entityType = entityType;
      rootPredicate = new FilterRootPredicate();
      predicates.push(rootPredicate);
   }

   @Override
   public void addComparisonPredicate(AggregationPropertyPath.Type aggregationType, List<String> propertyPath, ComparisonPredicate.Type comparisonType, Object value) {
      Object typedValue = propertyHelper.convertToBackendType(entityType, propertyPath, value);
      pushPredicate(new FilterComparisonPredicate(makePropertyValueExpr(entityType, propertyPath, aggregationType), comparisonType, typedValue));
   }

   @Override
   public void addRangePredicate(AggregationPropertyPath.Type aggregationType, List<String> propertyPath, Object lower, Object upper) {
      Object lowerValue = propertyHelper.convertToBackendType(entityType, propertyPath, lower);
      Object upperValue = propertyHelper.convertToBackendType(entityType, propertyPath, upper);
      pushPredicate(new FilterRangePredicate(makePropertyValueExpr(entityType, propertyPath, aggregationType), lowerValue, upperValue));
   }

   @Override
   public void addInPredicate(AggregationPropertyPath.Type aggregationType, List<String> propertyPath, List<Object> elements) {
      List<Object> typedElements = new ArrayList<Object>(elements.size());
      for (Object element : elements) {
         typedElements.add(propertyHelper.convertToBackendType(entityType, propertyPath, element));
      }
      pushPredicate(new FilterInPredicate(makePropertyValueExpr(entityType, propertyPath, aggregationType), typedElements));
   }

   @Override
   public void addLikePredicate(AggregationPropertyPath.Type aggregationType, List<String> propertyPath, String patternValue, Character escapeCharacter) {
      pushPredicate(new FilterLikePredicate(makePropertyValueExpr(entityType, propertyPath, aggregationType), patternValue, escapeCharacter));
   }

   @Override
   public void addIsNullPredicate(AggregationPropertyPath.Type aggregationType, List<String> propertyPath) {
      pushPredicate(new FilterIsNullPredicate(makePropertyValueExpr(entityType, propertyPath, aggregationType)));
   }

   @Override
   public void pushAndPredicate() {
      pushPredicate(new FilterConjunctionPredicate());
   }

   @Override
   public void pushOrPredicate() {
      pushPredicate(new FilterDisjunctionPredicate());
   }

   @Override
   public void pushNotPredicate() {
      pushPredicate(new FilterNegationPredicate());
   }

   private void pushPredicate(Predicate<BooleanExpr> predicate) {
      // Add as sub-predicate to the current top predicate
      predicates.peek().add(predicate);

      // push to parent predicate stack if required
      if (predicate.getType().isParent()) {
         @SuppressWarnings("unchecked")
         ParentPredicate<BooleanExpr> parentPredicate = predicate.as(ParentPredicate.class);
         predicates.push(parentPredicate);
      }
   }

   @Override
   public void popBooleanPredicate() {
      predicates.pop();
   }

   @Override
   public BooleanExpr build() {
      return rootPredicate.getQuery();
   }

   private PropertyValueExpr makePropertyValueExpr(String entityType, List<String> propertyPath, AggregationPropertyPath.Type aggregationType) {
      if (aggregationType != null) {
         return new AggregationExpr(PropertyPath.AggregationType.from(aggregationType), propertyPath, propertyHelper.isRepeatedProperty(entityType, propertyPath));
      } else {
         return new PropertyValueExpr(propertyPath, propertyHelper.isRepeatedProperty(entityType, propertyPath));
      }
   }

   @Override
   public String toString() {
      return "SingleEntityHavingQueryBuilderImpl[entityType=" + entityType + ", rootPredicate=" + rootPredicate + "]";
   }
}
