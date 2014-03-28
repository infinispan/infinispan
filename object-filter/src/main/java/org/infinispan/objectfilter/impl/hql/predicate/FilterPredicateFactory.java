package org.infinispan.objectfilter.impl.hql.predicate;

import org.hibernate.hql.ast.spi.EntityNamesResolver;
import org.hibernate.hql.ast.spi.predicate.*;
import org.hibernate.hql.ast.spi.predicate.ComparisonPredicate.Type;
import org.infinispan.objectfilter.impl.syntax.BooleanExpr;
import org.infinispan.objectfilter.impl.util.StringHelper;

import java.util.List;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public class FilterPredicateFactory implements PredicateFactory<BooleanExpr> {

   private final EntityNamesResolver entityNamesResolver;

   public FilterPredicateFactory(EntityNamesResolver entityNamesResolver) {
      this.entityNamesResolver = entityNamesResolver;
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
      return new FilterComparisonPredicate(getPathAsString(propertyPath), comparisonType, comparisonValue);
   }

   @Override
   public InPredicate<BooleanExpr> getInPredicate(String entityType, List<String> propertyPath, List<Object> values) {
      return new FilterInPredicate(getPathAsString(propertyPath), values);
   }

   @Override
   public RangePredicate<BooleanExpr> getRangePredicate(String entityType, List<String> propertyPath,
                                                        Object lowerValue, Object upperValue) {
      return new FilterRangePredicate(getPathAsString(propertyPath), lowerValue, upperValue);
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
      return new FilterLikePredicate(getPathAsString(propertyPath), patternValue);
   }

   @Override
   public IsNullPredicate<BooleanExpr> getIsNullPredicate(String entityType, List<String> propertyPath) {
      return new FilterIsNullPredicate(getPathAsString(propertyPath));
   }

   private String getPathAsString(List<String> propertyPath) {
      return StringHelper.join(propertyPath, ".");
   }
}
