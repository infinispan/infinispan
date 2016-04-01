package org.infinispan.objectfilter.impl.hql.predicate;

import org.hibernate.hql.ast.spi.EntityNamesResolver;
import org.hibernate.hql.ast.spi.predicate.ComparisonPredicate;
import org.hibernate.hql.ast.spi.predicate.ComparisonPredicate.Type;
import org.hibernate.hql.ast.spi.predicate.ConjunctionPredicate;
import org.hibernate.hql.ast.spi.predicate.DisjunctionPredicate;
import org.hibernate.hql.ast.spi.predicate.InPredicate;
import org.hibernate.hql.ast.spi.predicate.IsNullPredicate;
import org.hibernate.hql.ast.spi.predicate.LikePredicate;
import org.hibernate.hql.ast.spi.predicate.NegationPredicate;
import org.hibernate.hql.ast.spi.predicate.PredicateFactory;
import org.hibernate.hql.ast.spi.predicate.RangePredicate;
import org.hibernate.hql.ast.spi.predicate.RootPredicate;
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
      String[] path = propertyPath.toArray(new String[propertyPath.size()]);
      ValueExpr valueExpr = new PropertyValueExpr(path, propertyHelper.isRepeatedProperty(entityType, path), propertyHelper.getPrimitivePropertyType(entityType, path));
      return new FilterComparisonPredicate(valueExpr, comparisonType, comparisonValue);
   }

   @Override
   public InPredicate<BooleanExpr> getInPredicate(String entityType, List<String> propertyPath, List<Object> values) {
      String[] path = propertyPath.toArray(new String[propertyPath.size()]);
      ValueExpr valueExpr = new PropertyValueExpr(path, propertyHelper.isRepeatedProperty(entityType, path), propertyHelper.getPrimitivePropertyType(entityType, path));
      return new FilterInPredicate(valueExpr, values);
   }

   @Override
   public RangePredicate<BooleanExpr> getRangePredicate(String entityType, List<String> propertyPath,
                                                        Object lowerValue, Object upperValue) {
      String[] path = propertyPath.toArray(new String[propertyPath.size()]);
      ValueExpr valueExpr = new PropertyValueExpr(path, propertyHelper.isRepeatedProperty(entityType, path), propertyHelper.getPrimitivePropertyType(entityType, path));
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
      String[] path = propertyPath.toArray(new String[propertyPath.size()]);
      ValueExpr valueExpr = new PropertyValueExpr(path, propertyHelper.isRepeatedProperty(entityType, path), propertyHelper.getPrimitivePropertyType(entityType, path));
      return new FilterLikePredicate(valueExpr, patternValue, escapeCharacter);
   }

   @Override
   public IsNullPredicate<BooleanExpr> getIsNullPredicate(String entityType, List<String> propertyPath) {
      String[] path = propertyPath.toArray(new String[propertyPath.size()]);
      //todo [anistor] 2 calls to propertyHelper .. very inefficient
      ValueExpr valueExpr = new PropertyValueExpr(path, propertyHelper.isRepeatedProperty(entityType, path), propertyHelper.getPrimitivePropertyType(entityType, path));
      return new FilterIsNullPredicate(valueExpr);
   }
}
