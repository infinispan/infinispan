package org.infinispan.objectfilter.impl.syntax;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Expands a filter expression into a superset of it, using only indexed fields. The expanded expression is computed by
 * applying <a href="http://en.wikipedia.org/wiki/Boole%27s_expansion_theorem">Boole's expansion theorem</a> to all
 * non-indexed fields and then ignoring the non-indexed fields from the resulting product.
 *
 * @author anistor@redhat.com
 * @since 8.0
 */
public final class BooleShannonExpansion {

   // todo [anistor] besides indexed vs non-indexed we need to detect occurrences of cross-relationship spurious matches and apply a second in-memory filtering phase
   public interface IndexedFieldProvider {

      boolean isIndexed(List<String> propertyPath);
   }

   private final IndexedFieldProvider indexedFieldProvider;

   public BooleShannonExpansion(IndexedFieldProvider indexedFieldProvider) {
      this.indexedFieldProvider = indexedFieldProvider;
   }

   private static class Collector implements Visitor {

      private final IndexedFieldProvider indexedFieldProvider;

      private boolean foundIndexed = false;

      private final Set<PrimaryPredicateExpr> predicatesToRemove = new LinkedHashSet<PrimaryPredicateExpr>();

      private Collector(IndexedFieldProvider indexedFieldProvider) {
         this.indexedFieldProvider = indexedFieldProvider;
      }

      @Override
      public BooleanExpr visit(NotExpr notExpr) {
         notExpr.getChild().acceptVisitor(this);
         return notExpr;
      }

      @Override
      public BooleanExpr visit(OrExpr orExpr) {
         for (BooleanExpr c : orExpr.getChildren()) {
            c.acceptVisitor(this);
         }
         return orExpr;
      }

      @Override
      public BooleanExpr visit(AndExpr andExpr) {
         for (BooleanExpr c : andExpr.getChildren()) {
            c.acceptVisitor(this);
         }
         return andExpr;
      }

      @Override
      public BooleanExpr visit(ConstantBooleanExpr constantBooleanExpr) {
         return constantBooleanExpr;
      }

      @Override
      public BooleanExpr visit(IsNullExpr isNullExpr) {
         PropertyValueExpr propertyValueExpr = (PropertyValueExpr) isNullExpr.getChild();
         if (indexedFieldProvider.isIndexed(propertyValueExpr.getPropertyPath())) {
            foundIndexed = true;
         } else {
            predicatesToRemove.add(isNullExpr);
         }
         return isNullExpr;
      }

      @Override
      public BooleanExpr visit(ComparisonExpr comparisonExpr) {
         PropertyValueExpr propertyValueExpr = (PropertyValueExpr) comparisonExpr.getLeftChild();
         if (indexedFieldProvider.isIndexed(propertyValueExpr.getPropertyPath())) {
            foundIndexed = true;
         } else {
            predicatesToRemove.add(comparisonExpr);
         }
         return comparisonExpr;
      }

      @Override
      public BooleanExpr visit(LikeExpr likeExpr) {
         PropertyValueExpr propertyValueExpr = (PropertyValueExpr) likeExpr.getChild();
         if (indexedFieldProvider.isIndexed(propertyValueExpr.getPropertyPath())) {
            foundIndexed = true;
         } else {
            predicatesToRemove.add(likeExpr);
         }
         return likeExpr;
      }

      @Override
      public ValueExpr visit(ConstantValueExpr constantValueExpr) {
         return constantValueExpr;
      }

      @Override
      public ValueExpr visit(PropertyValueExpr propertyValueExpr) {
         return propertyValueExpr;
      }
   }

   private static class Replacer implements Visitor {

      private final PrimaryPredicateExpr toReplace;
      private final ConstantBooleanExpr with;
      private boolean found = false;

      private Replacer(PrimaryPredicateExpr toReplace, ConstantBooleanExpr with) {
         this.toReplace = toReplace;
         this.with = with;
      }

      @Override
      public BooleanExpr visit(NotExpr notExpr) {
         BooleanExpr transformedChild = notExpr.getChild().acceptVisitor(this);
         if (transformedChild instanceof ConstantBooleanExpr) {
            return ((ConstantBooleanExpr) transformedChild).negate();
         }
         return new NotExpr(transformedChild);
      }

      @Override
      public BooleanExpr visit(OrExpr orExpr) {
         List<BooleanExpr> newChildren = new ArrayList<BooleanExpr>(orExpr.getChildren().size());
         for (BooleanExpr c : orExpr.getChildren()) {
            BooleanExpr e = c.acceptVisitor(this);
            if (e instanceof ConstantBooleanExpr) {
               if (((ConstantBooleanExpr) e).getValue()) {
                  return ConstantBooleanExpr.TRUE;
               }
            } else {
               if (e instanceof OrExpr) {
                  newChildren.addAll(((OrExpr) e).getChildren());
               } else {
                  newChildren.add(e);
               }
            }
         }
         PredicateOptimisations.optimizePredicates(newChildren, true);
         if (newChildren.size() == 1) {
            return newChildren.get(0);
         }
         return new OrExpr(newChildren);
      }

      @Override
      public BooleanExpr visit(AndExpr andExpr) {
         List<BooleanExpr> newChildren = new ArrayList<BooleanExpr>(andExpr.getChildren().size());
         for (BooleanExpr c : andExpr.getChildren()) {
            BooleanExpr e = c.acceptVisitor(this);
            if (e instanceof ConstantBooleanExpr) {
               if (!((ConstantBooleanExpr) e).getValue()) {
                  return ConstantBooleanExpr.FALSE;
               }
            } else {
               if (e instanceof AndExpr) {
                  newChildren.addAll(((AndExpr) e).getChildren());
               } else {
                  newChildren.add(e);
               }
            }
         }
         PredicateOptimisations.optimizePredicates(newChildren, true);
         if (newChildren.size() == 1) {
            return newChildren.get(0);
         }
         return new AndExpr(newChildren);
      }

      @Override
      public BooleanExpr visit(ConstantBooleanExpr constantBooleanExpr) {
         return constantBooleanExpr;
      }

      @Override
      public BooleanExpr visit(IsNullExpr isNullExpr) {
         return replacePredicate(isNullExpr);
      }

      @Override
      public BooleanExpr visit(ComparisonExpr comparisonExpr) {
         return replacePredicate(comparisonExpr);
      }

      @Override
      public BooleanExpr visit(LikeExpr likeExpr) {
         return replacePredicate(likeExpr);
      }

      @Override
      public ValueExpr visit(ConstantValueExpr constantValueExpr) {
         return constantValueExpr;
      }

      @Override
      public ValueExpr visit(PropertyValueExpr propertyValueExpr) {
         return propertyValueExpr;
      }

      private BooleanExpr replacePredicate(PrimaryPredicateExpr primaryPredicateExpr) {
         switch (PredicateOptimisations.comparePrimaryPredicates(false, primaryPredicateExpr, false, toReplace)) {
            case 0:
               found = true;
               return with;
            case 1:
               found = true;
               return with.negate();
            default:
               return primaryPredicateExpr;
         }
      }
   }

   public BooleanExpr expand(BooleanExpr booleanExpr) {
      if (booleanExpr instanceof ConstantBooleanExpr) {
         return booleanExpr;
      }

      Collector collector = new Collector(indexedFieldProvider);
      booleanExpr.acceptVisitor(collector);

      if (!collector.foundIndexed) {
         return ConstantBooleanExpr.TRUE;
      }

      if (!collector.predicatesToRemove.isEmpty()) {
         for (PrimaryPredicateExpr e : collector.predicatesToRemove) {
            Replacer replacer1 = new Replacer(e, ConstantBooleanExpr.TRUE);
            BooleanExpr e1 = booleanExpr.acceptVisitor(replacer1);
            if (!replacer1.found) {
               continue;
            }
            if (e1 == ConstantBooleanExpr.TRUE) {
               return ConstantBooleanExpr.TRUE;
            }
            Replacer replacer2 = new Replacer(e, ConstantBooleanExpr.FALSE);
            BooleanExpr e2 = booleanExpr.acceptVisitor(replacer2);
            if (e2 == ConstantBooleanExpr.TRUE) {
               return ConstantBooleanExpr.TRUE;
            }
            if (e1 == ConstantBooleanExpr.FALSE) {
               booleanExpr = e2;
            } else if (e2 == ConstantBooleanExpr.FALSE) {
               booleanExpr = e1;
            } else {
               OrExpr disjunction;
               if (e1 instanceof OrExpr) {
                  disjunction = (OrExpr) e1;
                  if (e2 instanceof OrExpr) {
                     disjunction.getChildren().addAll(((OrExpr) e2).getChildren());
                  } else {
                     disjunction.getChildren().add(e2);
                  }
               } else if (e2 instanceof OrExpr) {
                  disjunction = (OrExpr) e2;
                  disjunction.getChildren().add(e1);
               } else {
                  disjunction = new OrExpr(e1, e2);
               }
               PredicateOptimisations.optimizePredicates(disjunction.getChildren(), false);
               booleanExpr = disjunction;
            }
         }
      }

      return booleanExpr;
   }
}
