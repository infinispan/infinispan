package org.infinispan.objectfilter.impl.syntax;

import java.util.List;

/**
 * @author anistor@redhat.com
 * @since 8.0
 */
final class PredicateOptimisations {

   /**
    * Checks if two predicates are identical or opposite.
    *
    * @param isFirstNegated  is first predicate negated?
    * @param first           the first predicate expression
    * @param isSecondNegated is second predicate negated?
    * @param second          the second predicate expression
    * @return -1 if unrelated predicates, 0 if identical predicates, 1 if opposite predicates
    */
   public static int comparePrimaryPredicates(boolean isFirstNegated, PrimaryPredicateExpr first, boolean isSecondNegated, PrimaryPredicateExpr second) {
      if (first.getClass() == second.getClass()) {
         if (first instanceof ComparisonExpr) {
            ComparisonExpr comparison1 = (ComparisonExpr) first;
            ComparisonExpr comparison2 = (ComparisonExpr) second;
            assert comparison1.getLeftChild() instanceof PropertyValueExpr;
            assert comparison1.getRightChild() instanceof ConstantValueExpr;
            assert comparison2.getLeftChild() instanceof PropertyValueExpr;
            assert comparison2.getRightChild() instanceof ConstantValueExpr;
            if (comparison1.getLeftChild().equals(comparison2.getLeftChild()) && comparison1.getRightChild().equals(comparison2.getRightChild())) {
               ComparisonExpr.Type cmpType1 = comparison1.getComparisonType();
               if (isFirstNegated) {
                  cmpType1 = cmpType1.negate();
               }
               ComparisonExpr.Type cmpType2 = comparison2.getComparisonType();
               if (isSecondNegated) {
                  cmpType2 = cmpType2.negate();
               }
               return cmpType1 == cmpType2 ? 0 : (cmpType1 == cmpType2.negate() ? 1 : -1);
            }
         } else if (first.equals(second)) {
            return isFirstNegated == isSecondNegated ? 0 : 1;
         }
      }
      return -1;
   }

   public static void optimizePredicates(List<BooleanExpr> children, boolean isConjunction) {
      removeRedundantPredicates(children, isConjunction);
      optimizeOverlappingIntervalPredicates(children, isConjunction);
   }

   /**
    * Removes duplicate occurrences of same predicate in a conjunction or disjunction. Also detects and removes
    * tautology and contradiction. The following translation rules are applied: <ul> <li>X || X => X</li> <li>X && X =>
    * X</li> <li>!X || !X => !X</li> <li>!X && !X => !X</li> <li>X || !X => TRUE (tautology)</li> <li>X && !X => FALSE
    * (contradiction)</li> </ul>
    *
    * @param children      the list of children expressions
    * @param isConjunction is the parent boolean expression a conjunction or a disjunction?
    */
   private static void removeRedundantPredicates(List<BooleanExpr> children, boolean isConjunction) {
      for (int i = 0; i < children.size(); i++) {
         BooleanExpr ci = children.get(i);
         if (ci instanceof BooleanOperatorExpr) {
            // we may encounter non-predicate expressions, just ignore them
            continue;
         }
         boolean isCiNegated = ci instanceof NotExpr;
         if (isCiNegated) {
            ci = ((NotExpr) ci).getChild();
         }
         assert ci instanceof PrimaryPredicateExpr;
         PrimaryPredicateExpr ci1 = (PrimaryPredicateExpr) ci;
         assert ci1.getChild() instanceof PropertyValueExpr;
         PropertyValueExpr pve = (PropertyValueExpr) ci1.getChild();
         if (pve.isRepeated()) {
            // do not optimize repeated predicates
            continue;
         }
         int j = i + 1;
         while (j < children.size()) {
            BooleanExpr cj = children.get(j);
            // we may encounter non-predicate expressions, just ignore them
            if (!(cj instanceof BooleanOperatorExpr)) {
               boolean isCjNegated = cj instanceof NotExpr;
               if (isCjNegated) {
                  cj = ((NotExpr) cj).getChild();
               }
               PrimaryPredicateExpr cj1 = (PrimaryPredicateExpr) cj;
               assert cj1.getChild() instanceof PropertyValueExpr;
               PropertyValueExpr pve2 = (PropertyValueExpr) cj1.getChild();
               // do not optimize repeated predicates
               if (!pve2.isRepeated()) {
                  int res = comparePrimaryPredicates(isCiNegated, ci1, isCjNegated, cj1);
                  if (res == 0) {
                     // found duplication
                     children.remove(j);
                     continue;
                  } else if (res == 1) {
                     // found tautology or contradiction
                     children.clear();
                     children.add(ConstantBooleanExpr.forBoolean(!isConjunction));
                     return;
                  }
               }
            }
            j++;
         }
      }
   }

   private static void optimizeOverlappingIntervalPredicates(List<BooleanExpr> children, boolean isConjunction) {
      for (int i = 0; i < children.size(); i++) {
         BooleanExpr ci = children.get(i);
         if (ci instanceof ComparisonExpr) {
            ComparisonExpr first = (ComparisonExpr) ci;
            assert first.getLeftChild() instanceof PropertyValueExpr;
            assert first.getRightChild() instanceof ConstantValueExpr;

            PropertyValueExpr pve = (PropertyValueExpr) first.getLeftChild();
            if (pve.isRepeated()) {
               // do not optimize repeated predicates
               continue;
            }

            int j = i + 1;
            while (j < children.size()) {
               BooleanExpr cj = children.get(j);
               if (cj instanceof ComparisonExpr) {
                  ComparisonExpr second = (ComparisonExpr) cj;
                  assert second.getLeftChild() instanceof PropertyValueExpr;
                  assert second.getRightChild() instanceof ConstantValueExpr;

                  PropertyValueExpr pve2 = (PropertyValueExpr) second.getLeftChild();
                  // do not optimize repeated predicates
                  if (!pve2.isRepeated()) {
                     if (first.getLeftChild().equals(second.getLeftChild())) {
                        BooleanExpr res = optimizeOverlappingIntervalPredicates(first, second, isConjunction);
                        if (res != null) {
                           if (res instanceof ConstantBooleanExpr) {
                              children.clear();
                              children.add(res);
                              return;
                           }
                           children.remove(j);
                           if (res != first) {
                              first = (ComparisonExpr) res;
                              children.set(i, first);
                           }
                           continue;
                        }
                     }
                  }
               }
               j++;
            }
         }
      }
   }

   /**
    * @param first
    * @param second
    * @param isConjunction
    * @return null or a replacement BooleanExpr
    */
   private static BooleanExpr optimizeOverlappingIntervalPredicates(ComparisonExpr first, ComparisonExpr second, boolean isConjunction) {
      final Comparable firstValue = ((ConstantValueExpr) first.getRightChild()).getConstantValue();
      final Comparable secondValue = ((ConstantValueExpr) second.getRightChild()).getConstantValue();
      final int cmp = firstValue.compareTo(secondValue);

      if (first.getComparisonType() == ComparisonExpr.Type.EQUAL) {
         return optimizeEqAndInterval(first, second, isConjunction, cmp);
      } else if (second.getComparisonType() == ComparisonExpr.Type.EQUAL) {
         return optimizeEqAndInterval(second, first, isConjunction, -cmp);
      } else if (first.getComparisonType() == ComparisonExpr.Type.NOT_EQUAL) {
         return optimizeNotEqAndInterval(first, second, isConjunction, cmp);
      } else if (second.getComparisonType() == ComparisonExpr.Type.NOT_EQUAL) {
         return optimizeNotEqAndInterval(second, first, isConjunction, -cmp);
      }

      if (cmp == 0) {
         if (first.getComparisonType() == second.getComparisonType()) {
            // identical intervals
            return first;
         }
         if (first.getComparisonType() == second.getComparisonType().negate()) {
            // opposite directions, disjoint, union is full coverage
            return isConjunction ? ConstantBooleanExpr.FALSE : ConstantBooleanExpr.TRUE;
         }
         if (first.getComparisonType() == ComparisonExpr.Type.LESS_OR_EQUAL || first.getComparisonType() == ComparisonExpr.Type.GREATER_OR_EQUAL) {
            // opposite directions, overlapping in one point, union is full coverage
            return isConjunction ? new ComparisonExpr(first.getLeftChild(), first.getRightChild(), ComparisonExpr.Type.EQUAL) : ConstantBooleanExpr.TRUE;
         } else {
            // opposite directions, disjoint in one point, union is not full coverage
            return isConjunction ? ConstantBooleanExpr.FALSE : new ComparisonExpr(first.getLeftChild(), first.getRightChild(), ComparisonExpr.Type.NOT_EQUAL);
         }
      }

      // opposite direction intervals
      if (first.getComparisonType() == second.getComparisonType().negate() || first.getComparisonType() == second.getComparisonType().reverse()) {
         if (cmp < 0) {
            if (first.getComparisonType() == ComparisonExpr.Type.LESS || first.getComparisonType() == ComparisonExpr.Type.LESS_OR_EQUAL) {
               if (isConjunction) {
                  return ConstantBooleanExpr.FALSE;
               }
            } else if (!isConjunction) {
               return ConstantBooleanExpr.TRUE;
            }
         } else {
            if (first.getComparisonType() == ComparisonExpr.Type.GREATER || first.getComparisonType() == ComparisonExpr.Type.GREATER_OR_EQUAL) {
               if (isConjunction) {
                  return ConstantBooleanExpr.FALSE;
               }
            } else if (!isConjunction) {
               return ConstantBooleanExpr.TRUE;
            }
         }
         return null;
      }

      // same direction intervals
      if (first.getComparisonType() == ComparisonExpr.Type.LESS || first.getComparisonType() == ComparisonExpr.Type.LESS_OR_EQUAL) {
         // less than
         if (isConjunction) {
            return cmp < 0 ? first : second;
         } else {
            return cmp < 0 ? second : first;
         }
      } else {
         // greater than
         if (isConjunction) {
            return cmp < 0 ? second : first;
         } else {
            return cmp < 0 ? first : second;
         }
      }
   }

   private static BooleanExpr optimizeEqAndInterval(ComparisonExpr first, ComparisonExpr second, boolean isConjunction, int cmp) {
      assert first.getComparisonType() == ComparisonExpr.Type.EQUAL;

      switch (second.getComparisonType()) {
         case EQUAL:
            if (cmp == 0) {
               return first;
            }
            return isConjunction ? ConstantBooleanExpr.FALSE : null;
         case NOT_EQUAL:
            if (cmp == 0) {
               return ConstantBooleanExpr.forBoolean(!isConjunction);
            }
            return isConjunction ? first : null;
         case LESS:
            if (cmp == 0) {
               return isConjunction ? ConstantBooleanExpr.FALSE : new ComparisonExpr(first.getLeftChild(), first.getRightChild(), ComparisonExpr.Type.LESS_OR_EQUAL);
            }
            if (cmp < 0) {
               return isConjunction ? first : second;
            }
            return isConjunction ? ConstantBooleanExpr.FALSE : null;
         case LESS_OR_EQUAL:
            if (cmp <= 0) {
               return isConjunction ? first : second;
            }
            return isConjunction ? ConstantBooleanExpr.FALSE : null;
         case GREATER:
            if (cmp == 0) {
               return isConjunction ? ConstantBooleanExpr.FALSE : new ComparisonExpr(first.getLeftChild(), first.getRightChild(), ComparisonExpr.Type.GREATER_OR_EQUAL);
            }
            if (cmp > 0) {
               return isConjunction ? first : second;
            }
            return isConjunction ? ConstantBooleanExpr.FALSE : null;
         case GREATER_OR_EQUAL:
            if (cmp >= 0) {
               return isConjunction ? first : second;
            }
            return isConjunction ? ConstantBooleanExpr.FALSE : null;
         default:
            return null;
      }
   }

   private static BooleanExpr optimizeNotEqAndInterval(ComparisonExpr first, ComparisonExpr second, boolean isConjunction, int cmp) {
      assert first.getComparisonType() == ComparisonExpr.Type.NOT_EQUAL;

      switch (second.getComparisonType()) {
         case EQUAL:
            if (cmp == 0) {
               return ConstantBooleanExpr.FALSE;
            }
            return isConjunction ? second : first;
         case NOT_EQUAL:
            if (cmp == 0) {
               return first;
            }
            return isConjunction ? null : ConstantBooleanExpr.TRUE;
         case LESS:
            if (cmp >= 0) {
               return isConjunction ? second : first;
            }
            return isConjunction ? null : ConstantBooleanExpr.TRUE;
         case LESS_OR_EQUAL:
            if (cmp < 0) {
               return isConjunction ? null : ConstantBooleanExpr.TRUE;
            }
            if (cmp > 0) {
               return isConjunction ? second : first;
            }
            return isConjunction ? new ComparisonExpr(first.getLeftChild(), first.getRightChild(), ComparisonExpr.Type.LESS) : ConstantBooleanExpr.TRUE;
         case GREATER:
            if (cmp > 0) {
               return isConjunction ? null : ConstantBooleanExpr.TRUE;
            }
            return isConjunction ? second : first;
         case GREATER_OR_EQUAL:
            if (cmp < 0) {
               return isConjunction ? second : first;
            }
            if (cmp > 0) {
               return isConjunction ? new ComparisonExpr(first.getLeftChild(), first.getRightChild(), ComparisonExpr.Type.GREATER) : ConstantBooleanExpr.TRUE;
            }
            return isConjunction ? ConstantBooleanExpr.FALSE : null;
         default:
            return null;
      }
   }
}
