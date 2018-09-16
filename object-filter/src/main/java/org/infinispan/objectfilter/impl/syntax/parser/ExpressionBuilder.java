package org.infinispan.objectfilter.impl.syntax.parser;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

import org.infinispan.objectfilter.impl.logging.Log;
import org.infinispan.objectfilter.impl.ql.PropertyPath;
import org.infinispan.objectfilter.impl.ql.QueryRendererDelegate;
import org.infinispan.objectfilter.impl.syntax.AggregationExpr;
import org.infinispan.objectfilter.impl.syntax.AndExpr;
import org.infinispan.objectfilter.impl.syntax.BetweenExpr;
import org.infinispan.objectfilter.impl.syntax.BooleanExpr;
import org.infinispan.objectfilter.impl.syntax.ComparisonExpr;
import org.infinispan.objectfilter.impl.syntax.ConstantBooleanExpr;
import org.infinispan.objectfilter.impl.syntax.ConstantValueExpr;
import org.infinispan.objectfilter.impl.syntax.FullTextBoostExpr;
import org.infinispan.objectfilter.impl.syntax.FullTextOccurExpr;
import org.infinispan.objectfilter.impl.syntax.FullTextRangeExpr;
import org.infinispan.objectfilter.impl.syntax.FullTextRegexpExpr;
import org.infinispan.objectfilter.impl.syntax.FullTextTermExpr;
import org.infinispan.objectfilter.impl.syntax.IsNullExpr;
import org.infinispan.objectfilter.impl.syntax.LikeExpr;
import org.infinispan.objectfilter.impl.syntax.NotExpr;
import org.infinispan.objectfilter.impl.syntax.OrExpr;
import org.infinispan.objectfilter.impl.syntax.PropertyValueExpr;
import org.jboss.logging.Logger;

/**
 * Builder for the creation of WHERE/HAVING clause filters targeting a single entity.
 * <p>
 * Implemented as a stack of {@link LazyBooleanExpr}s which allows to add elements to the constructed query in a
 * uniform manner while traversing through the original query parse tree.
 *
 * @author anistor@redhat.com
 * @since 9.0
 */
final class ExpressionBuilder<TypeMetadata> {

   private static final Log log = Logger.getMessageLogger(Log.class, ExpressionBuilder.class.getName());

   private final ObjectPropertyHelper<TypeMetadata> propertyHelper;

   private TypeMetadata entityType;

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

   public void addFullTextTerm(PropertyPath<?> propertyPath, String value, Integer fuzzySlop) {
      push(new FullTextTermExpr(makePropertyValueExpr(propertyPath), value, fuzzySlop));
   }

   public void addFullTextRegexp(PropertyPath<?> propertyPath, String regexp) {
      push(new FullTextRegexpExpr(makePropertyValueExpr(propertyPath), regexp));
   }

   public void addFullTextRange(PropertyPath<?> propertyPath, boolean includeLower, Object lower, Object upper, boolean includeUpper) {
      push(new FullTextRangeExpr(makePropertyValueExpr(propertyPath), includeLower, lower, upper, includeUpper));
   }

   public void addComparison(PropertyPath<?> propertyPath, ComparisonExpr.Type comparisonType, Object value) {
      Comparable typedValue = (Comparable) propertyHelper.convertToBackendType(entityType, propertyPath.asArrayPath(), value);
      push(new ComparisonExpr(makePropertyValueExpr(propertyPath), new ConstantValueExpr(typedValue), comparisonType));
   }

   public void addRange(PropertyPath<?> propertyPath, Object lower, Object upper) {
      Comparable lowerValue = (Comparable) propertyHelper.convertToBackendType(entityType, propertyPath.asArrayPath(), lower);
      Comparable upperValue = (Comparable) propertyHelper.convertToBackendType(entityType, propertyPath.asArrayPath(), upper);
      push(new BetweenExpr(makePropertyValueExpr(propertyPath), new ConstantValueExpr(lowerValue), new ConstantValueExpr(upperValue)));
   }

   public void addIn(PropertyPath<?> propertyPath, List<Object> values) {
      PropertyValueExpr valueExpr = makePropertyValueExpr(propertyPath);
      List<BooleanExpr> children = new ArrayList<>(values.size());
      for (Object element : values) {  //todo [anistor] need more efficient implementation
         Comparable typedValue = (Comparable) propertyHelper.convertToBackendType(entityType, propertyPath.asArrayPath(), element);
         ComparisonExpr booleanExpr = new ComparisonExpr(valueExpr, new ConstantValueExpr(typedValue), ComparisonExpr.Type.EQUAL);
         children.add(booleanExpr);
      }
      push(new OrExpr(children));
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
