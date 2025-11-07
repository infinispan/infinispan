package org.infinispan.query.objectfilter.impl.syntax.parser;

import java.lang.invoke.MethodHandles;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.infinispan.commons.util.Util;
import org.infinispan.query.objectfilter.impl.logging.Log;
import org.infinispan.query.objectfilter.impl.ql.PropertyPath;
import org.infinispan.query.objectfilter.impl.ql.QueryRendererDelegate;
import org.infinispan.query.objectfilter.impl.syntax.AggregationExpr;
import org.infinispan.query.objectfilter.impl.syntax.AndExpr;
import org.infinispan.query.objectfilter.impl.syntax.BetweenExpr;
import org.infinispan.query.objectfilter.impl.syntax.BooleanExpr;
import org.infinispan.query.objectfilter.impl.syntax.ComparisonExpr;
import org.infinispan.query.objectfilter.impl.syntax.ConstantBooleanExpr;
import org.infinispan.query.objectfilter.impl.syntax.ConstantValueExpr;
import org.infinispan.query.objectfilter.impl.syntax.FullTextBoostExpr;
import org.infinispan.query.objectfilter.impl.syntax.FullTextOccurExpr;
import org.infinispan.query.objectfilter.impl.syntax.FullTextRangeExpr;
import org.infinispan.query.objectfilter.impl.syntax.FullTextRegexpExpr;
import org.infinispan.query.objectfilter.impl.syntax.FullTextTermExpr;
import org.infinispan.query.objectfilter.impl.syntax.IsNullExpr;
import org.infinispan.query.objectfilter.impl.syntax.KnnPredicate;
import org.infinispan.query.objectfilter.impl.syntax.LikeExpr;
import org.infinispan.query.objectfilter.impl.syntax.NestedExpr;
import org.infinispan.query.objectfilter.impl.syntax.NotExpr;
import org.infinispan.query.objectfilter.impl.syntax.OrExpr;
import org.infinispan.query.objectfilter.impl.syntax.PropertyValueExpr;
import org.infinispan.query.objectfilter.impl.syntax.SpatialWithinBoxExpr;
import org.infinispan.query.objectfilter.impl.syntax.SpatialWithinCircleExpr;
import org.infinispan.query.objectfilter.impl.syntax.SpatialWithinPolygonExpr;
import org.jboss.logging.Logger;

/**
 * Builder for the creation of WHERE/HAVING clause filters targeting a single entity.
  * Implemented as a stack of {@link LazyBooleanExpr}s which allows to add elements to the constructed query in a
 * uniform manner while traversing through the original query parse tree.
 *
 * @author anistor@redhat.com
 * @since 9.0
 */
final class ExpressionBuilder<TypeMetadata> {

   private static final Log log = Logger.getMessageLogger(MethodHandles.lookup(), Log.class, ExpressionBuilder.class.getName());

   private final ObjectPropertyHelper<TypeMetadata> propertyHelper;

   private TypeMetadata entityType;

   private final Map<String, NestedExpr> nestedExprMap = new HashMap<>();

   /**
    * Keep track of all the parent expressions ({@code AND}, {@code OR}, {@code NOT}) of the WHERE/HAVING clause of the
    * built query.
    */
   private final Deque<LazyBooleanExpr> stack = new ArrayDeque<>();

   ExpressionBuilder(ObjectPropertyHelper<TypeMetadata> propertyHelper) {
      this.propertyHelper = propertyHelper;
   }

   public void setEntityType(TypeMetadata entityType) {
      this.entityType = entityType;
      stack.push(new LazyRootBooleanExpr());
   }

   public void addFullTextTerm(PropertyPath<?> propertyPath, Object comparisonObject, Integer fuzzySlop) {
      push(new FullTextTermExpr(makePropertyValueExpr(propertyPath), comparisonObject, fuzzySlop));
   }

   public void addFullTextRegexp(PropertyPath<?> propertyPath, String regexp) {
      push(new FullTextRegexpExpr(makePropertyValueExpr(propertyPath), regexp));
   }

   public void addFullTextRange(PropertyPath<?> propertyPath, boolean includeLower, Object lower, Object upper, boolean includeUpper) {
      push(new FullTextRangeExpr(makePropertyValueExpr(propertyPath), includeLower, lower, upper, includeUpper));
   }

   public void addKnnPredicate(PropertyPath<?> propertyPath, Class<?> expectedType, List<Object> vector, Object knn) {
      push(new KnnPredicate(makePropertyValueExpr(propertyPath), expectedType, vector, knn));
   }

   public void addKnnPredicate(PropertyPath<?> propertyPath, Class<?> expectedType, ConstantValueExpr.ParamPlaceholder vector, Object knn) {
      push(new KnnPredicate(makePropertyValueExpr(propertyPath), expectedType, vector, knn));
   }

   public void addComparison(PropertyPath<?> propertyPath, ComparisonExpr.Type comparisonType, Object value) {
      Comparable typedValue = (Comparable) propertyHelper.convertToBackendType(entityType, propertyPath.asArrayPath(), value);
      ComparisonExpr comparisonExpr = new ComparisonExpr(makePropertyValueExpr(propertyPath), new ConstantValueExpr(typedValue), comparisonType);
      push(comparisonExpr);
   }

   public void addNestedComparison(PropertyPath<?> propertyPath, ComparisonExpr.Type comparisonType, Object value, String joinAlias, PropertyPath<TypeDescriptor<TypeMetadata>> embeddedPath) {
      if (!propertyHelper.isNestedIndexStructure(entityType, embeddedPath.getNodeNamesWithoutAlias().toArray(Util.EMPTY_STRING_ARRAY))) {
         log.warn("NestedExpr currently only supported on NESTED embedded fields. Falling back to ComparisonExpr");
         addComparison(propertyPath, comparisonType, value);
         return;
      }
      Comparable typedValue = (Comparable) propertyHelper.convertToBackendType(entityType, propertyPath.asArrayPath(), value);

      ComparisonExpr comparisonExpr = new ComparisonExpr(makePropertyValueExpr(propertyPath), new ConstantValueExpr(typedValue), comparisonType);
      if (!nestedExprMap.containsKey(joinAlias)) {
         nestedExprMap.put(joinAlias, new NestedExpr(getNestedPath(propertyPath)));
         push(nestedExprMap.get(joinAlias));
      } else if (stack.peek() instanceof LazyNegationExpr) {
         push(nestedExprMap.get(joinAlias));
      }
      nestedExprMap.get(joinAlias).add(comparisonExpr);
   }

   private String getNestedPath(PropertyPath<?> propertyPath) {
      String stringPath = propertyPath.asStringPath();
      return stringPath.substring(0, stringPath.contains(".") ? stringPath.lastIndexOf(".") : stringPath.length());
   }

   public void addRange(PropertyPath<?> propertyPath, Object lower, Object upper) {
      Comparable lowerValue = (Comparable) propertyHelper.convertToBackendType(entityType, propertyPath.asArrayPath(), lower);
      Comparable upperValue = (Comparable) propertyHelper.convertToBackendType(entityType, propertyPath.asArrayPath(), upper);
      push(new BetweenExpr(makePropertyValueExpr(propertyPath), new ConstantValueExpr(lowerValue), new ConstantValueExpr(upperValue)));
   }

   public void addSpatialWithinCircle(PropertyPath<?> propertyPath, Object lat, Object lon, Object radius, Object unit) {
      push(new SpatialWithinCircleExpr(makePropertyValueExpr(propertyPath),
            new ConstantValueExpr((Comparable) lat),
            new ConstantValueExpr((Comparable) lon),
            new ConstantValueExpr((Comparable) radius),
            new ConstantValueExpr((Comparable) unit)));
   }

   public void addSpatialWithinBox(PropertyPath<?> propertyPath, Object tlLat, Object tlLon, Object brLat, Object brLon) {
      push(new SpatialWithinBoxExpr(makePropertyValueExpr(propertyPath),
            new ConstantValueExpr((Comparable) tlLat),
            new ConstantValueExpr((Comparable) tlLon),
            new ConstantValueExpr((Comparable) brLat),
            new ConstantValueExpr((Comparable) brLon)
      ));
   }

   public void addSpatialWithinPolygon(PropertyPath<?> propertyPath, List<Object> vector) {
      push(new SpatialWithinPolygonExpr(makePropertyValueExpr(propertyPath),
            vector.stream().map(item -> new ConstantValueExpr((Comparable) item)).toList()));
   }

   public void addIn(PropertyPath<?> propertyPath, List<Object> values) {
      PropertyValueExpr valueExpr = makePropertyValueExpr(propertyPath);
      List<BooleanExpr> children = new ArrayList<>(values.size());
      for (Object element : values) {  //todo [anistor] need more efficient implementation
         Comparable typedValue = (Comparable) propertyHelper.convertToBackendType(entityType, propertyPath.asArrayPath(), element);
         ComparisonExpr booleanExpr = new ComparisonExpr(valueExpr, new ConstantValueExpr(typedValue), ComparisonExpr.Type.EQUAL);
         children.add(booleanExpr);
      }
      if (children.size() == 1) {
         // simplify INs with just one clause by removing the wrapper boolean expression
         push(children.get(0));
      } else {
         push(new OrExpr(children));
      }
   }

   public void addLike(PropertyPath<?> propertyPath, Object patternValue, Character escapeCharacter) {
      // TODO [anistor] escapeCharacter is ignored for now
      push(new LikeExpr(makePropertyValueExpr(propertyPath), patternValue));
   }

   public void addIsNull(PropertyPath<?> propertyPath) {
      push(new IsNullExpr(makePropertyValueExpr(propertyPath)));
   }

   public void pushAnd() {
      push(new LazyAndExpr());
   }

   public void pushOr() {
      push(new LazyOrExpr());
   }

   public void pushNot() {
      push(new LazyNegationExpr());
   }

   public void pushFullTextBoost(float boost) {
      push(new LazyFTBoostExpr(boost));
   }

   public void pushFullTextOccur(QueryRendererDelegate.Occur occur) {
      push(new LazyFTOccurExpr(occur));
   }

   private void push(BooleanExpr expr) {
      push(new LazyLeafBooleanExpr(expr));
   }

   private void push(LazyBooleanExpr expr) {
      // add as sub-expression to the current top expression
      stack.peek().addChild(expr);

      // push to expression stack if required
      if (expr.isParent()) {
         stack.push(expr);
      }
   }

   public void pop() {
      stack.pop();
      if(!nestedExprMap.isEmpty() && stack.peek() instanceof LazyOrExpr){
         nestedExprMap.clear();
      }
   }

   public BooleanExpr build() {
      return stack.getFirst().get();
   }

   private PropertyValueExpr makePropertyValueExpr(PropertyPath<?> propertyPath) {
      //todo [anistor] 2 calls to propertyHelper .. very inefficient
      boolean isRepeated = propertyHelper.isRepeatedProperty(entityType, propertyPath.asArrayPath());
      Class<?> primitiveType = propertyHelper.getPrimitivePropertyType(entityType, propertyPath.asArrayPath());
      if (propertyPath instanceof AggregationPropertyPath) {
         return new AggregationExpr((AggregationPropertyPath) propertyPath, isRepeated, primitiveType);
      } else {
         return new PropertyValueExpr(propertyPath, isRepeated, primitiveType);
      }
   }

   public void addConstantBoolean(boolean booleanConstant) {
      push(ConstantBooleanExpr.forBoolean(booleanConstant));
   }

   /**
    * Delays construction until {@link #get} is called.
    */
   abstract static class LazyBooleanExpr {

      /**
       * Indicates whether this expression can have sub-expressions or not.
       */
      public boolean isParent() {
         return true;
      }

      /**
       * Adds the given lazy expression to this.
       *
       * @param child the child to add
       */
      public abstract void addChild(LazyBooleanExpr child);

      /**
       * Returns the query represented by this lazy expression. Contains the all sub-expressions if this is a parent.
       *
       * @return the expression represented by this lazy expression
       */
      public abstract BooleanExpr get();
   }

   private static final class LazyLeafBooleanExpr extends LazyBooleanExpr {

      private final BooleanExpr booleanExpr;

      LazyLeafBooleanExpr(BooleanExpr booleanExpr) {
         this.booleanExpr = booleanExpr;
      }

      @Override
      public boolean isParent() {
         return false;
      }

      @Override
      public void addChild(LazyBooleanExpr child) {
         throw new UnsupportedOperationException("Adding a sub-expression to a non-parent expression is illegal");
      }

      @Override
      public BooleanExpr get() {
         return booleanExpr;
      }
   }

   private static final class LazyAndExpr extends LazyBooleanExpr {

      private final List<LazyBooleanExpr> children = new ArrayList<>(3);

      @Override
      public void addChild(LazyBooleanExpr child) {
         children.add(child);
      }

      @Override
      public BooleanExpr get() {
         if (children.isEmpty()) {
            throw new IllegalStateException("A conjunction must have at least one child");
         }
         BooleanExpr firstChild = children.get(0).get();
         if (children.size() == 1) {
            return firstChild;
         }

         AndExpr andExpr = new AndExpr(firstChild);

         for (int i = 1; i < children.size(); i++) {
            BooleanExpr child = children.get(i).get();
            andExpr.getChildren().add(child);
         }

         return andExpr;
      }
   }

   private static final class LazyOrExpr extends LazyBooleanExpr {

      private final List<LazyBooleanExpr> children = new ArrayList<>(3);

      @Override
      public void addChild(LazyBooleanExpr child) {
         children.add(child);
      }

      @Override
      public BooleanExpr get() {
         if (children.isEmpty()) {
            throw new IllegalStateException("A disjunction must have at least one child");
         }
         BooleanExpr firstChild = children.get(0).get();
         if (children.size() == 1) {
            return firstChild;
         }

         OrExpr orExpr = new OrExpr(firstChild);

         for (int i = 1; i < children.size(); i++) {
            BooleanExpr child = children.get(i).get();
            orExpr.getChildren().add(child);
         }

         return orExpr;
      }
   }

   private static final class LazyNegationExpr extends LazyBooleanExpr {

      private LazyBooleanExpr child;

      @Override
      public void addChild(LazyBooleanExpr child) {
         if (this.child != null) {
            throw log.getNotMoreThanOnePredicateInNegationAllowedException(child);
         }
         this.child = child;
      }

      public LazyBooleanExpr getChild() {
         return child;
      }

      @Override
      public BooleanExpr get() {
         return new NotExpr(getChild().get());
      }
   }

   private static final class LazyFTBoostExpr extends LazyBooleanExpr {

      private LazyBooleanExpr child;

      private final float boost;

      LazyFTBoostExpr(float boost) {
         this.boost = boost;
      }

      @Override
      public void addChild(LazyBooleanExpr child) {
         if (this.child != null) {
            throw log.getNotMoreThanOnePredicateInNegationAllowedException(child);
         }
         this.child = child;
      }

      public LazyBooleanExpr getChild() {
         return child;
      }

      @Override
      public BooleanExpr get() {
         return new FullTextBoostExpr(getChild().get(), boost);
      }
   }

   private static final class LazyFTOccurExpr extends LazyBooleanExpr {

      private LazyBooleanExpr child;

      private final QueryRendererDelegate.Occur occur;

      LazyFTOccurExpr(QueryRendererDelegate.Occur occur) {
         this.occur = occur;
      }

      @Override
      public void addChild(LazyBooleanExpr child) {
         if (this.child != null) {
            throw log.getNotMoreThanOnePredicateInNegationAllowedException(child);
         }
         this.child = child;
      }

      public LazyBooleanExpr getChild() {
         return child;
      }

      @Override
      public BooleanExpr get() {
         return new FullTextOccurExpr(getChild().get(), occur);
      }
   }

   private static final class LazyRootBooleanExpr extends LazyBooleanExpr {

      private LazyBooleanExpr child;

      @Override
      public void addChild(LazyBooleanExpr child) {
         if (this.child != null) {
            throw log.getNotMoreThanOnePredicateInRootOfWhereClauseAllowedException(child);
         }
         this.child = child;
      }

      @Override
      public BooleanExpr get() {
         return child == null ? null : child.get();
      }
   }
}
