package org.infinispan.objectfilter.impl.predicateindex.be;

import org.infinispan.objectfilter.impl.MetadataAdapter;
import org.infinispan.objectfilter.impl.predicateindex.EqualsCondition;
import org.infinispan.objectfilter.impl.predicateindex.IntervalPredicate;
import org.infinispan.objectfilter.impl.predicateindex.IsNullCondition;
import org.infinispan.objectfilter.impl.predicateindex.Predicate;
import org.infinispan.objectfilter.impl.predicateindex.RegexCondition;
import org.infinispan.objectfilter.impl.syntax.*;
import org.infinispan.objectfilter.impl.util.Interval;

import java.util.ArrayList;
import java.util.List;

/**
 * Creates a BETree out of a BooleanExpr.
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class BETreeMaker<AttributeId extends Comparable<AttributeId>> {

   private final MetadataAdapter<?, ?, AttributeId> metadataAdapter;

   private final boolean useIntervals;

   public BETreeMaker(MetadataAdapter<?, ?, AttributeId> metadataAdapter, boolean useIntervals) {
      this.metadataAdapter = metadataAdapter;
      this.useIntervals = useIntervals;
   }

   public BETree make(BooleanExpr booleanExpr) {
      List<BENode> nodes = new ArrayList<BENode>();
      List<Integer> treeCounters = new ArrayList<Integer>();

      if (booleanExpr instanceof ConstantBooleanExpr) {
         treeCounters.add(((ConstantBooleanExpr) booleanExpr).getValue() ? BETree.EXPR_TRUE : BETree.EXPR_FALSE);
      } else {
         preorderTraversal(null, booleanExpr, nodes, treeCounters);
      }

      int[] countersArray = new int[treeCounters.size()];
      for (int i = 0; i < countersArray.length; i++) {
         countersArray[i] = treeCounters.get(i);
      }
      return new BETree(nodes.toArray(new BENode[nodes.size()]), countersArray);
   }

   private void preorderTraversal(BENode parent, BooleanExpr child, List<BENode> nodes, List<Integer> treeCounters) {
      if (child instanceof NotExpr) {
         PrimaryPredicateExpr condition = (PrimaryPredicateExpr) ((NotExpr) child).getChild();
         makePredicateNode(parent, nodes, treeCounters, condition, true);
      } else if (child instanceof PrimaryPredicateExpr) {
         PrimaryPredicateExpr condition = (PrimaryPredicateExpr) child;
         makePredicateNode(parent, nodes, treeCounters, condition, false);
      } else if (child instanceof OrExpr) {
         makeBooleanOperatorNode((OrExpr) child, nodes, treeCounters, new OrNode(parent));
      } else if (child instanceof AndExpr) {
         makeBooleanOperatorNode((AndExpr) child, nodes, treeCounters, new AndNode(parent));
      } else {
         throw new IllegalStateException("Unexpected *Expr node type: " + child);
      }
   }

   private void makePredicateNode(BENode parent, List<BENode> nodes, List<Integer> treeCounters, PrimaryPredicateExpr condition, boolean isNegated) {
      List<String> propertyPath = ((PropertyValueExpr) condition.getChild()).getPropertyPath();
      List<AttributeId> path = metadataAdapter.translatePropertyPath(propertyPath);
      boolean isRepeated = metadataAdapter.isRepeatedProperty(propertyPath);

      if (condition instanceof ComparisonExpr) {
         ComparisonExpr expr = (ComparisonExpr) condition;
         ConstantValueExpr right = (ConstantValueExpr) expr.getRightChild();
         switch (expr.getComparisonType()) {
            case NOT_EQUAL:
               if (useIntervals) {
                  if (!(parent instanceof OrNode)) {
                     parent = new OrNode(parent);
                     int size = nodes.size();
                     parent.setLocation(size, size + 4);
                     nodes.add(parent);
                     treeCounters.add(3);
                  }
                  // the special case of non-equality is transformed into two intervals, excluding the compared value, + an IS NULL predicate
                  addPredicateNode(parent, nodes, treeCounters, isNegated, path, new IntervalPredicate(isRepeated, new Interval(Interval.getMinusInf(), false, right.getConstantValue(), false)));
                  addPredicateNode(parent, nodes, treeCounters, isNegated, path, new IntervalPredicate(isRepeated, new Interval(right.getConstantValue(), false, Interval.getPlusInf(), false)));
                  addPredicateNode(parent, nodes, treeCounters, isNegated, path, new Predicate<Object>(isRepeated, IsNullCondition.INSTANCE));
               } else {
                  addPredicateNode(parent, nodes, treeCounters, !isNegated, path, new Predicate<Object>(isRepeated, new EqualsCondition(right.getConstantValue())));
               }
               break;
            case EQUAL:
               if (useIntervals) {
                  addPredicateNode(parent, nodes, treeCounters, isNegated, path, new IntervalPredicate(isRepeated, new Interval(right.getConstantValue(), true, right.getConstantValue(), true)));
               } else {
                  addPredicateNode(parent, nodes, treeCounters, isNegated, path, new Predicate<Object>(isRepeated, new EqualsCondition(right.getConstantValue())));
               }
               break;
            case LESS:
               addPredicateNode(parent, nodes, treeCounters, isNegated, path, new IntervalPredicate(isRepeated, new Interval(Interval.getMinusInf(), false, right.getConstantValue(), false)));
               break;
            case LESS_OR_EQUAL:
               addPredicateNode(parent, nodes, treeCounters, isNegated, path, new IntervalPredicate(isRepeated, new Interval(Interval.getMinusInf(), false, right.getConstantValue(), true)));
               break;
            case GREATER:
               addPredicateNode(parent, nodes, treeCounters, isNegated, path, new IntervalPredicate(isRepeated, new Interval(right.getConstantValue(), false, Interval.getPlusInf(), false)));
               break;
            case GREATER_OR_EQUAL:
               addPredicateNode(parent, nodes, treeCounters, isNegated, path, new IntervalPredicate(isRepeated, new Interval(right.getConstantValue(), true, Interval.getPlusInf(), false)));
               break;
            default:
               throw new IllegalStateException("Unexpected comparison type: " + expr.getComparisonType());
         }
      } else if (condition instanceof IsNullExpr) {
         addPredicateNode(parent, nodes, treeCounters, isNegated, path, new Predicate<Object>(isRepeated, IsNullCondition.INSTANCE));
      } else if (condition instanceof LikeExpr) {
         addPredicateNode(parent, nodes, treeCounters, isNegated, path, new Predicate<String>(isRepeated, new RegexCondition(((LikeExpr) condition).getPattern())));
      } else {
         throw new IllegalStateException("Unexpected condition type: " + condition);
      }
   }

   private void addPredicateNode(BENode parent, List<BENode> nodes, List<Integer> treeCounters, boolean isNegated, List<AttributeId> path, Predicate predicate) {
      PredicateNode predicateNode = new PredicateNode<AttributeId>(parent, predicate, isNegated, path);
      int size = nodes.size();
      predicateNode.setLocation(size, size + 1);
      nodes.add(predicateNode);
      treeCounters.add(1);
   }

   private void makeBooleanOperatorNode(BooleanOperatorExpr child, List<BENode> nodes, List<Integer> treeCounters, BENode node) {
      int index = nodes.size();
      nodes.add(node);
      List<BooleanExpr> children = child.getChildren();
      treeCounters.add(children.size());
      for (BooleanExpr c : children) {
         preorderTraversal(node, c, nodes, treeCounters);
      }
      node.setLocation(index, nodes.size());
   }
}
