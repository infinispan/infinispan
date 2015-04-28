package org.infinispan.objectfilter.impl.syntax;

import java.util.ArrayList;
import java.util.List;

/**
 * Applies some optimisations to a boolean expression. Most notably, it brings it to NNF (Negation normal form, see
 * http://en.wikipedia.org/wiki/Negation_normal_form). Moves negation directly near the variable by repeatedly applying
 * the De Morgan's laws (see http://en.wikipedia.org/wiki/De_Morgan%27s_laws). Eliminates double negation. Normalizes
 * comparison operators by replacing 'greater' with 'less'. Detects sub-expressions that are boolean constants.
 * Simplifies boolean constants by applying boolean short-circuiting. Eliminates resulting trivial conjunctions or
 * disjunctions that have only one child. Ensures all paths from root to leafs contain an alternation of conjunction and
 * disjunction. This is achieved by absorbing the children whenever a boolean sub-expression if of the same kind as the
 * parent.
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class BooleanFilterNormalizer {

   /**
    * A visitor that removes constant boolean expressions, absorbs sub-expressions (removes needless parentheses) and
    * swaps comparison operand sides to ensure the constant is always on the right side.
    */
   private final Visitor simplifierVisitor = new NoOpVisitor() {

      @Override
      public BooleanExpr visit(NotExpr notExpr) {
         // push the negation down the tree until it reaches a PrimaryPredicateExpr
         return notExpr.getChild().acceptVisitor(deMorganVisitor);
      }

      @Override
      public BooleanExpr visit(OrExpr orExpr) {
         List<BooleanExpr> children = new ArrayList<BooleanExpr>(orExpr.getChildren().size());

         for (BooleanExpr child : orExpr.getChildren()) {
            child = child.acceptVisitor(this);

            if (child instanceof ConstantBooleanExpr) {
               // remove boolean constants or shortcircuit entirely
               if (((ConstantBooleanExpr) child).getValue()) {
                  return ConstantBooleanExpr.TRUE;
               }
            } else if (child instanceof OrExpr) {
               // absorb sub-expressions of the same kind
               children.addAll(((OrExpr) child).getChildren());
            } else {
               children.add(child);
            }
         }

         optimizePredicates(children, false);

         // simplify trivial expressions
         if (children.size() == 1) {
            return children.get(0);
         }

         return new OrExpr(children);
      }

      @Override
      public BooleanExpr visit(AndExpr andExpr) {
         List<BooleanExpr> children = new ArrayList<BooleanExpr>(andExpr.getChildren().size());

         for (BooleanExpr child : andExpr.getChildren()) {
            child = child.acceptVisitor(this);

            if (child instanceof ConstantBooleanExpr) {
               // remove boolean constants or shortcircuit entirely
               if (!((ConstantBooleanExpr) child).getValue()) {
                  return ConstantBooleanExpr.FALSE;
               }
            } else if (child instanceof AndExpr) {
               // absorb sub-expressions of the same kind
               children.addAll(((AndExpr) child).getChildren());
            } else {
               children.add(child);
            }
         }

         optimizePredicates(children, true);

         // simplify trivial expressions
         if (children.size() == 1) {
            return children.get(0);
         }

         return new AndExpr(children);
      }

      private void optimizePredicates(List<BooleanExpr> children, boolean isConjunction) {
         removeRedundantPredicates(children, isConjunction);
         optimizeOverlappingIntervalPredicates(children, isConjunction);
      }

      /**
       * Removes duplicate occurrences of same predicate in a conjunction or disjunction. Also detects and removes tautology and contradiction.
       * The following translation rules are applied:
       * <ul>
       * <li>X || X => X</li>
       * <li>X && X => X</li>
       * <li>!X || !X => !X</li>
       * <li>!X && !X => !X</li>
       * <li>X || !X => TRUE (tautology)</li>
       * <li>X && !X => FALSE (contradiction)</li>
       * </ul>
       * @param children the list of children expressions
       * @param isConjunction is the parent boolean expression a conjunction or a disjunction?
       */
      private void removeRedundantPredicates(List<BooleanExpr> children, boolean isConjunction) {
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
                     int res = comparePrimaryPredicateExpr(isCiNegated, ci1, isCjNegated, cj1);
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

      /**
       * Checks if two predicates are identical or opposite.
       *
       * @param isFirstNegated is first predicate negated?
       * @param first the first predicate expression
       * @param isSecondNegated is second predicate negated?
       * @param second the second predicate expression
       * @return -1 if unrelated predicates, 0 if identical predicates, 1 if opposite predicates
       */
      private int comparePrimaryPredicateExpr(boolean isFirstNegated, PrimaryPredicateExpr first, boolean isSecondNegated, PrimaryPredicateExpr second) {
         if (first.getClass() == second.getClass()) {
            if (first instanceof ComparisonExpr) {
               ComparisonExpr comparison1 = (ComparisonExpr) first;
               ComparisonExpr comparison2 = (ComparisonExpr) second;
               assert comparison1.getLeftChild() instanceof PropertyValueExpr;
               assert comparison1.getRightChild() instanceof ConstantValueExpr;
               assert comparison2.getLeftChild() instanceof PropertyValueExpr;
               assert comparison2.getRightChild() instanceof ConstantValueExpr;
               assert !isFirstNegated;
               assert !isSecondNegated;
               if (comparison1.getLeftChild().equals(comparison2.getLeftChild()) && comparison1.getRightChild().equals(comparison2.getRightChild())) {
                  ComparisonExpr.Type cmpType1 = comparison1.getComparisonType();
                  ComparisonExpr.Type cmpType2 = comparison2.getComparisonType();
                  return cmpType1 == cmpType2 ? 0 : (cmpType1 == cmpType2.negate() ? 1 : -1);
               }
            } else if (first.equals(second)) {
               return isFirstNegated == isSecondNegated ? 0 : 1;
            }
         }
         return -1;
      }

      private void optimizeOverlappingIntervalPredicates(List<BooleanExpr> children, boolean isConjunction) {
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
      private BooleanExpr optimizeOverlappingIntervalPredicates(ComparisonExpr first, ComparisonExpr second, boolean isConjunction) {
         Comparable firstValue = ((ConstantValueExpr) first.getRightChild()).getConstantValue();
         Comparable secondValue = ((ConstantValueExpr) second.getRightChild()).getConstantValue();
         int cmp = firstValue.compareTo(secondValue);

         if (first.getComparisonType() == ComparisonExpr.Type.EQUAL) {
            return eqAndInterval(first, second, isConjunction, cmp);
         } else if (second.getComparisonType() == ComparisonExpr.Type.EQUAL) {
            return eqAndInterval(second, first, isConjunction, -cmp);
         } else if (first.getComparisonType() == ComparisonExpr.Type.NOT_EQUAL) {
            return notEqAndInterval(first, second, isConjunction, cmp);
         } else if (second.getComparisonType() == ComparisonExpr.Type.NOT_EQUAL) {
            return notEqAndInterval(second, first, isConjunction, -cmp);
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

      @Override
      public BooleanExpr visit(ComparisonExpr comparisonExpr) {
         // start moving the constant to the right side of the comparison if it's not already there
         ValueExpr leftChild = comparisonExpr.getLeftChild();
         leftChild = leftChild.acceptVisitor(this);

         ValueExpr rightChild = comparisonExpr.getRightChild();
         rightChild = rightChild.acceptVisitor(this);

         ComparisonExpr.Type comparisonType = comparisonExpr.getComparisonType();

         // handle constant expressions
         if (leftChild instanceof ConstantValueExpr) {
            if (rightChild instanceof ConstantValueExpr) {
               // replace the comparison of the two constants with the actual result
               Comparable leftValue = ((ConstantValueExpr) leftChild).getConstantValue();
               Comparable rightValue = ((ConstantValueExpr) rightChild).getConstantValue();
               int compRes = leftValue.compareTo(rightValue);
               switch (comparisonType) {
                  case LESS:
                     return ConstantBooleanExpr.forBoolean(compRes < 0);
                  case LESS_OR_EQUAL:
                     return ConstantBooleanExpr.forBoolean(compRes <= 0);
                  case EQUAL:
                     return ConstantBooleanExpr.forBoolean(compRes == 0);
                  case NOT_EQUAL:
                     return ConstantBooleanExpr.forBoolean(compRes != 0);
                  case GREATER_OR_EQUAL:
                     return ConstantBooleanExpr.forBoolean(compRes >= 0);
                  case GREATER:
                     return ConstantBooleanExpr.forBoolean(compRes > 0);
                  default:
                     throw new IllegalStateException("Unexpected comparison type: " + comparisonType);
               }
            }

            // swap operand sides to ensure the constant is always on the right side
            ValueExpr temp = rightChild;
            rightChild = leftChild;
            leftChild = temp;

            // now reverse the operator too to restore the semantics
            comparisonType = comparisonType.reverse();
         }

         // comparison operators are never negated using NotExpr
         return new ComparisonExpr(leftChild, rightChild, comparisonType);
      }
   };

   private BooleanExpr eqAndInterval(ComparisonExpr first, ComparisonExpr second, boolean isConjunction, int cmp) {
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

   private BooleanExpr notEqAndInterval(ComparisonExpr first, ComparisonExpr second, boolean isConjunction, int cmp) {
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

   /**
    * Handles negation by applying De Morgan laws.
    */
   private final Visitor deMorganVisitor = new NoOpVisitor() {

      @Override
      public BooleanExpr visit(ConstantBooleanExpr constantBooleanExpr) {
         // negated constants are simplified immediately
         return constantBooleanExpr.negate();
      }

      @Override
      public BooleanExpr visit(NotExpr notExpr) {
         // double negation is eliminated, child is simplified
         return notExpr.getChild().acceptVisitor(simplifierVisitor);
      }

      @Override
      public BooleanExpr visit(OrExpr orExpr) {
         List<BooleanExpr> children = new ArrayList<BooleanExpr>(orExpr.getChildren().size());

         for (BooleanExpr child : orExpr.getChildren()) {
            child = child.acceptVisitor(this);

            if (child instanceof ConstantBooleanExpr) {
               // remove boolean constants
               if (!((ConstantBooleanExpr) child).getValue()) {
                  return ConstantBooleanExpr.FALSE;
               }
            } else if (child instanceof AndExpr) {
               // absorb sub-expressions of the same kind
               children.addAll(((AndExpr) child).getChildren());
            } else {
               children.add(child);
            }
         }

         // simplify trivial expressions
         if (children.size() == 1) {
            return children.get(0);
         }

         return new AndExpr(children);
      }

      @Override
      public BooleanExpr visit(AndExpr andExpr) {
         List<BooleanExpr> children = new ArrayList<BooleanExpr>(andExpr.getChildren().size());

         for (BooleanExpr child : andExpr.getChildren()) {
            child = child.acceptVisitor(this);

            if (child instanceof ConstantBooleanExpr) {
               // remove boolean constants
               if (((ConstantBooleanExpr) child).getValue()) {
                  return ConstantBooleanExpr.TRUE;
               }
            } else if (child instanceof OrExpr) {
               // absorb sub-expressions of the same kind
               children.addAll(((OrExpr) child).getChildren());
            } else {
               children.add(child);
            }
         }

         // simplify trivial expressions
         if (children.size() == 1) {
            return children.get(0);
         }

         return new OrExpr(children);
      }

      @Override
      public BooleanExpr visit(ComparisonExpr comparisonExpr) {
         BooleanExpr booleanExpr = comparisonExpr.acceptVisitor(simplifierVisitor);

         // simplify negated constants immediately
         if (booleanExpr instanceof ConstantBooleanExpr) {
            return ((ConstantBooleanExpr) booleanExpr).negate();
         }

         // eliminate double negation
         if (booleanExpr instanceof NotExpr) {
            return ((NotExpr) booleanExpr).getChild();
         }

         // interval predicates are never negated, they are converted instead into the opposite interval
         if (booleanExpr instanceof ComparisonExpr) {
            ComparisonExpr c = (ComparisonExpr) booleanExpr;
            return new ComparisonExpr(c.getLeftChild(), c.getRightChild(), c.getComparisonType().negate());
         }

         return new NotExpr(booleanExpr);
      }

      @Override
      public BooleanExpr visit(IsNullExpr isNullExpr) {
         return new NotExpr(isNullExpr);
      }

      @Override
      public BooleanExpr visit(LikeExpr likeExpr) {
         return new NotExpr(likeExpr);
      }
   };

   public BooleanExpr normalize(BooleanExpr booleanExpr) {
      return booleanExpr.acceptVisitor(simplifierVisitor);
   }
}
