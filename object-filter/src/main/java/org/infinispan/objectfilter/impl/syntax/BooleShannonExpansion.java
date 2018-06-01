package org.infinispan.objectfilter.impl.syntax;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Expands an input filter expression composed of indexed and unindexed fields into a superset of it
 * (matching the set of the input filter plus some more false positives), using only indexed fields.
 * The expanded expression is computed by applying <a href="http://en.wikipedia.org/wiki/Boole%27s_expansion_theorem">Boole's expansion theorem</a>
 * to all non-indexed fields and then ignoring the non-indexed fields from the resulting product. The resulting product
 * is a more complex expression but it can be executed fully indexed. In some exterme cases it can become a tautology
 * (TRUE), indicating that the filter should be executed fully non-indexed by doing a full scan.
 *
 * @author anistor@redhat.com
 * @since 8.0
 */
public final class BooleShannonExpansion {

   /**
    * The maximum number of cofactors we allow in the product before giving up.
    */
   private final int maxExpansionCofactors;

   // todo [anistor] besides indexed vs non-indexed we need to detect occurrences of cross-relationship spurious matches and apply a second in-memory filtering phase

   private final IndexedFieldProvider.FieldIndexingMetadata fieldIndexingMetadata;

   public BooleShannonExpansion(int maxExpansionCofactors, IndexedFieldProvider.FieldIndexingMetadata fieldIndexingMetadata) {
      this.maxExpansionCofactors = maxExpansionCofactors;
      this.fieldIndexingMetadata = fieldIndexingMetadata;
   }

   private class Collector extends ExprVisitor {

      private boolean foundIndexed = false;

      private final Set<PrimaryPredicateExpr> predicatesToRemove = new LinkedHashSet<>();

      @Override
      public BooleanExpr visit(FullTextBoostExpr fullTextBoostExpr) {
         fullTextBoostExpr.getChild().acceptVisitor(this);
         return fullTextBoostExpr;
      }

      @Override
      public BooleanExpr visit(FullTextOccurExpr fullTextOccurExpr) {
         fullTextOccurExpr.getChild().acceptVisitor(this);
         return fullTextOccurExpr;
      }

      @Override
      public BooleanExpr visit(FullTextTermExpr fullTextTermExpr) {
         PropertyValueExpr propertyValueExpr = (PropertyValueExpr) fullTextTermExpr.getChild();
         if (fieldIndexingMetadata.isIndexed(propertyValueExpr.getPropertyPath().asArrayPath())) {
            foundIndexed = true;
         } else {
            predicatesToRemove.add(fullTextTermExpr);
         }
         return fullTextTermExpr;
      }

      @Override
      public BooleanExpr visit(FullTextRegexpExpr fullTextRegexpExpr) {
         PropertyValueExpr propertyValueExpr = (PropertyValueExpr) fullTextRegexpExpr.getChild();
         if (fieldIndexingMetadata.isIndexed(propertyValueExpr.getPropertyPath().asArrayPath())) {
            foundIndexed = true;
         } else {
            predicatesToRemove.add(fullTextRegexpExpr);
         }
         return fullTextRegexpExpr;
      }

      @Override
      public BooleanExpr visit(FullTextRangeExpr fullTextRangeExpr) {
         PropertyValueExpr propertyValueExpr = (PropertyValueExpr) fullTextRangeExpr.getChild();
         if (fieldIndexingMetadata.isIndexed(propertyValueExpr.getPropertyPath().asArrayPath())) {
            foundIndexed = true;
         } else {
            predicatesToRemove.add(fullTextRangeExpr);
         }
         return fullTextRangeExpr;
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
      public BooleanExpr visit(IsNullExpr isNullExpr) {
         PropertyValueExpr propertyValueExpr = (PropertyValueExpr) isNullExpr.getChild();
         if (fieldIndexingMetadata.isIndexed(propertyValueExpr.getPropertyPath().asArrayPath())) {
            foundIndexed = true;
         } else {
            predicatesToRemove.add(isNullExpr);
         }
         return isNullExpr;
      }

      @Override
      public BooleanExpr visit(ComparisonExpr comparisonExpr) {
         PropertyValueExpr propertyValueExpr = (PropertyValueExpr) comparisonExpr.getLeftChild();
         if (fieldIndexingMetadata.isIndexed(propertyValueExpr.getPropertyPath().asArrayPath())) {
            foundIndexed = true;
         } else {
            predicatesToRemove.add(comparisonExpr);
         }
         return comparisonExpr;
      }

      @Override
      public BooleanExpr visit(LikeExpr likeExpr) {
         PropertyValueExpr propertyValueExpr = (PropertyValueExpr) likeExpr.getChild();
         if (fieldIndexingMetadata.isIndexed(propertyValueExpr.getPropertyPath().asArrayPath())) {
            foundIndexed = true;
         } else {
            predicatesToRemove.add(likeExpr);
         }
         return likeExpr;
      }
   }

   private static class Replacer extends ExprVisitor {

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
         List<BooleanExpr> newChildren = new ArrayList<>(orExpr.getChildren().size());
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
         PredicateOptimisations.optimizePredicates(newChildren, false);
         if (newChildren.size() == 1) {
            return newChildren.get(0);
         }
         return new OrExpr(newChildren);
      }

      @Override
      public BooleanExpr visit(AndExpr andExpr) {
         List<BooleanExpr> newChildren = new ArrayList<>(andExpr.getChildren().size());
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

   /**
    * Creates a less restrictive (expanded) query that matches the same objects as the input query plus potentially some
    * more (false positives). This query can be executed fully indexed and the result can be filtered in a second pass
    * to remove the false positives. This method can eventually return TRUE and in that case the expansion is useless
    * and it is better to just run the entire query unindexed (full scan).
    * <p>
    * If all fields used by the input query are indexed then the expansion is identical to the input query.
    *
    * @param booleanExpr the expression to expand
    * @return the expanded query if some of the fields are non-indexed or the input query if all fields are indexed
    */
   public BooleanExpr expand(BooleanExpr booleanExpr) {
      if (booleanExpr == null || booleanExpr instanceof ConstantBooleanExpr) {
         return booleanExpr;
      }

      Collector collector = new Collector();
      booleanExpr.acceptVisitor(collector);

      if (!collector.foundIndexed) {
         return ConstantBooleanExpr.TRUE;
      }

      if (!collector.predicatesToRemove.isEmpty()) {
         int numCofactors = 1;
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
               numCofactors *= 2;
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
            if (numCofactors > maxExpansionCofactors) {
               // expansion is too big, it's better to do full scan rather than search the index with a huge and
               // complex query that is a disjunction of many predicates so will very likely match everything anyway
               return ConstantBooleanExpr.TRUE;
            }
         }
      }

      return booleanExpr;
   }
}
