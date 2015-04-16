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

         removeRedundantPredicates(children, false);

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

         removeRedundantPredicates(children, true);

         // simplify trivial expressions
         if (children.size() == 1) {
            return children.get(0);
         }

         return new AndExpr(children);
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
            int j = i + 1;
            while (j < children.size()) {
               BooleanExpr cj = children.get(j);
               // we may encounter non-predicate expressions, just ignore them
               if (!(cj instanceof BooleanOperatorExpr)) {
                  boolean isCjNegated = cj instanceof NotExpr;
                  if (isCjNegated) {
                     cj = ((NotExpr) cj).getChild();
                  }
                  int res = comparePrimaryPredicateExpr(isCiNegated, (PrimaryPredicateExpr) ci, isCjNegated, (PrimaryPredicateExpr) cj);
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
               assert comparison2.getLeftChild() instanceof PropertyValueExpr;
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

         // interval predicates cannot be negated, they are converted instead into the opposite interval
         if (booleanExpr instanceof ComparisonExpr) {
            ComparisonExpr c = (ComparisonExpr) booleanExpr;
            switch (c.getComparisonType()) {
               case LESS:
                  return new ComparisonExpr(c.getLeftChild(), c.getRightChild(), ComparisonExpr.Type.GREATER_OR_EQUAL);
               case LESS_OR_EQUAL:
                  return new ComparisonExpr(c.getLeftChild(), c.getRightChild(), ComparisonExpr.Type.GREATER);
               case GREATER_OR_EQUAL:
                  return new ComparisonExpr(c.getLeftChild(), c.getRightChild(), ComparisonExpr.Type.LESS);
               case GREATER:
                  return new ComparisonExpr(c.getLeftChild(), c.getRightChild(), ComparisonExpr.Type.LESS_OR_EQUAL);
               case EQUAL:
                  return new ComparisonExpr(c.getLeftChild(), c.getRightChild(), ComparisonExpr.Type.NOT_EQUAL);
               case NOT_EQUAL:
                  return new ComparisonExpr(c.getLeftChild(), c.getRightChild(), ComparisonExpr.Type.EQUAL);
               default:
                  throw new IllegalStateException("Unexpected comparison type: " + c.getComparisonType());
            }
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
