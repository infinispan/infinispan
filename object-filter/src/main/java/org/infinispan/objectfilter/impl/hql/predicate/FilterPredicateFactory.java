package org.infinispan.objectfilter.impl.hql.predicate;

import org.hibernate.hql.ast.spi.EntityNamesResolver;
import org.hibernate.hql.ast.spi.predicate.*;
import org.hibernate.hql.ast.spi.predicate.ComparisonPredicate.Type;
import org.infinispan.objectfilter.impl.hql.ObjectPropertyHelper;
import org.infinispan.objectfilter.impl.syntax.BooleanExpr;
import org.infinispan.objectfilter.impl.syntax.PropertyValueExpr;
import org.infinispan.objectfilter.impl.syntax.ValueExpr;

import java.util.List;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class FilterPredicateFactory implements PredicateFactory<BooleanExpr> {

   private final EntityNamesResolver entityNamesResolver;

   private final ObjectPropertyHelper propertyHelper;

   public FilterPredicateFactory(EntityNamesResolver entityNamesResolver, ObjectPropertyHelper propertyHelper) {
      this.entityNamesResolver = entityNamesResolver;
      this.propertyHelper = propertyHelper;
   }

   @Override
   public RootPredicate<BooleanExpr> getRootPredicate(String entityType) {
      if (entityNamesResolver.getClassFromName(entityType) == null) {
         throw new IllegalStateException("Unknown entity name " + entityType);
      }
      return new FilterRootPredicate();
   }

   @Override
   public ComparisonPredicate<BooleanExpr> getComparisonPredicate(String entityType, Type comparisonType,
                                                                  List<String> propertyPath, Object comparisonValue) {
      ValueExpr valueExpr = new PropertyValueExpr(propertyPath, propertyHelper.isRepeatedProperty(entityType, propertyPath));
      return new FilterComparisonPredicate(valueExpr, comparisonType, comparisonValue);
   }

   @Override
   public InPredicate<BooleanExpr> getInPredicate(String entityType, List<String> propertyPath, List<Object> values) {
      ValueExpr valueExpr = new PropertyValueExpr(propertyPath, propertyHelper.isRepeatedProperty(entityType, propertyPath));
      return new FilterInPredicate(valueExpr, values);
   }

   @Override
   public RangePredicate<BooleanExpr> getRangePredicate(String entityType, List<String> propertyPath,
                                                        Object lowerValue, Object upperValue) {
      ValueExpr valueExpr = new PropertyValueExpr(propertyPath, propertyHelper.isRepeatedProperty(entityType, propertyPath));
      return new FilterRangePredicate(valueExpr, lowerValue, upperValue);
   }

   @Override
   public NegationPredicate<BooleanExpr> getNegationPredicate() {
      return new FilterNegationPredicate();
   }

   @Override
   public DisjunctionPredicate<BooleanExpr> getDisjunctionPredicate() {
      return new FilterDisjunctionPredicate();
   }

   @Override
   public ConjunctionPredicate<BooleanExpr> getConjunctionPredicate() {
      return new FilterConjunctionPredicate();
   }

   @Override
   public LikePredicate<BooleanExpr> getLikePredicate(String entityType, List<String> propertyPath,
                                                      String patternValue, Character escapeCharacter) {
      ValueExpr valueExpr = new PropertyValueExpr(propertyPath, propertyHelper.isRepeatedProperty(entityType, propertyPath));
      return new FilterLikePredicate(valueExpr, patternValue, escapeCharacter);
   }

   @Override
   public IsNullPredicate<BooleanExpr> getIsNullPredicate(String entityType, List<String> propertyPath) {
      ValueExpr valueExpr = new PropertyValueExpr(propertyPath, propertyHelper.isRepeatedProperty(entityType, propertyPath));
      return new FilterIsNullPredicate(valueExpr);
   }
}
