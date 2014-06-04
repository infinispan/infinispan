package org.infinispan.objectfilter.impl.syntax;

import org.hibernate.hql.ast.spi.predicate.ComparisonPredicate;

import java.util.ArrayList;
import java.util.List;

/**
 * Applies some optimisations to a boolean expression. Most notably, it brings it to NNF (Negation normal form, see
 * http://en.wikipedia.org/wiki/Negation_normal_form). Moves negation directly near the variable by repeatedly applying
 * the DeMorgan laws. Eliminates double negation. Normalizes comparison operators by replacing 'greater' with 'less'.
 * Detects sub-expressions that are boolean constants. Simplifies boolean constants by applying boolean
 * short-circuiting. Eliminates resulting trivial conjunctions or disjunctions that have only one child. Ensures all
 * paths from root to leafs contain an alternation of conjunction and disjunction. This is achieved by absorbing the
 * children whenever a boolean sub-expression if of the same kind as the parent.
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class BooleanFilterNormalizer {

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
      public BooleanExpr visit(AndExpr andExpr) {
         List<BooleanExpr> children = new ArrayList<BooleanExpr>(andExpr.getChildren().size());

         for (BooleanExpr child : andExpr.getChildren()) {
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
      public BooleanExpr visit(ComparisonExpr comparisonExpr) {
         // start moving the constant to the right side of the comparison if it's not already there
         ValueExpr leftChild = comparisonExpr.getLeftChild();
         leftChild = leftChild.acceptVisitor(this);

         ValueExpr rightChild = comparisonExpr.getRightChild();
         rightChild = rightChild.acceptVisitor(this);

         ComparisonPredicate.Type comparisonType = comparisonExpr.getComparisonType();

         if (leftChild instanceof ConstantValueExpr) {
            if (rightChild instanceof ConstantValueExpr) {
               // remove the comparison of the two constants with the actual result
               Comparable leftValue = (Comparable) ((ConstantValueExpr) leftChild).getConstantValue();
               Comparable rightValue = (Comparable) ((ConstantValueExpr) rightChild).getConstantValue();
               int compRes = leftValue.compareTo(rightValue);
               switch (comparisonType) {
                  case LESS:
                     return ConstantBooleanExpr.forBoolean(compRes < 0);
                  case LESS_OR_EQUAL:
                     return ConstantBooleanExpr.forBoolean(compRes <= 0);
                  case EQUALS:
                     return ConstantBooleanExpr.forBoolean(compRes == 0);
                  case GREATER_OR_EQUAL:
                     return ConstantBooleanExpr.forBoolean(compRes >= 0);
                  case GREATER:
                     return ConstantBooleanExpr.forBoolean(compRes > 0);
                  default:
                     throw new IllegalStateException("Unknown comparison type: " + comparisonType);
               }
            }

            // swap sides to ensure the constant is on the right side
            ValueExpr temp = rightChild;
            rightChild = leftChild;
            leftChild = temp;

            // reverse the operator too
            switch (comparisonType) {
               case LESS:
                  comparisonType = ComparisonPredicate.Type.GREATER;
                  break;
               case LESS_OR_EQUAL:
                  comparisonType = ComparisonPredicate.Type.GREATER_OR_EQUAL;
                  break;
               case GREATER_OR_EQUAL:
                  comparisonType = ComparisonPredicate.Type.LESS_OR_EQUAL;
                  break;
               case GREATER:
                  comparisonType = ComparisonPredicate.Type.LESS;
                  break;
               default:
                  throw new IllegalStateException("Unknown comparison type: " + comparisonType);
            }
         }

         // comparison operators are never negated
         return new ComparisonExpr(leftChild, rightChild, comparisonType);
      }
   };

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
                  return new ComparisonExpr(c.getLeftChild(), c.getRightChild(), ComparisonPredicate.Type.GREATER_OR_EQUAL);
               case LESS_OR_EQUAL:
                  return new ComparisonExpr(c.getLeftChild(), c.getRightChild(), ComparisonPredicate.Type.GREATER);
               case GREATER_OR_EQUAL:
                  return new ComparisonExpr(c.getLeftChild(), c.getRightChild(), ComparisonPredicate.Type.LESS);
               case GREATER:
                  return new ComparisonExpr(c.getLeftChild(), c.getRightChild(), ComparisonPredicate.Type.LESS_OR_EQUAL);
               case EQUALS:
                  // the special case of equality is transformed into two intervals, excluding the compared value
                  return new OrExpr(new ComparisonExpr(c.getLeftChild(), c.getRightChild(), ComparisonPredicate.Type.LESS),
                                    new ComparisonExpr(c.getLeftChild(), c.getRightChild(), ComparisonPredicate.Type.GREATER));
               default:
                  throw new IllegalStateException("Unknown comparison type: " + c.getComparisonType());
            }
         }

         return new NotExpr(booleanExpr);
      }

      @Override
      public BooleanExpr visit(IsNullExpr isNullExpr) {
         return new NotExpr(isNullExpr);
      }

      @Override
      public BooleanExpr visit(RegexExpr regexExpr) {
         return new NotExpr(regexExpr);
      }
   };

   public BooleanExpr normalize(BooleanExpr booleanExpr) {
      return booleanExpr.acceptVisitor(simplifierVisitor);
   }
}
