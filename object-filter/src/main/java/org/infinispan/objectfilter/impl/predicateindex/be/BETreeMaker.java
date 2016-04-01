package org.infinispan.objectfilter.impl.predicateindex.be;

import org.infinispan.objectfilter.impl.MetadataAdapter;
import org.infinispan.objectfilter.impl.predicateindex.EqualsCondition;
import org.infinispan.objectfilter.impl.predicateindex.IntervalPredicate;
import org.infinispan.objectfilter.impl.predicateindex.IsNullCondition;
import org.infinispan.objectfilter.impl.predicateindex.Predicate;
import org.infinispan.objectfilter.impl.predicateindex.RegexCondition;
import org.infinispan.objectfilter.impl.syntax.AndExpr;
import org.infinispan.objectfilter.impl.syntax.BooleanExpr;
import org.infinispan.objectfilter.impl.syntax.BooleanOperatorExpr;
import org.infinispan.objectfilter.impl.syntax.ComparisonExpr;
import org.infinispan.objectfilter.impl.syntax.ConstantBooleanExpr;
import org.infinispan.objectfilter.impl.syntax.ConstantValueExpr;
import org.infinispan.objectfilter.impl.syntax.IsNullExpr;
import org.infinispan.objectfilter.impl.syntax.LikeExpr;
import org.infinispan.objectfilter.impl.syntax.NotExpr;
import org.infinispan.objectfilter.impl.syntax.OrExpr;
import org.infinispan.objectfilter.impl.syntax.PrimaryPredicateExpr;
import org.infinispan.objectfilter.impl.syntax.PropertyValueExpr;
import org.infinispan.objectfilter.impl.util.Interval;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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

   public BETree make(BooleanExpr booleanExpr, Map<String, Object> namedParameters) {
      List<BENode> nodes = new ArrayList<>();
      List<Integer> treeCounters = new ArrayList<>();

      if (booleanExpr == null) {
         treeCounters.add(BETree.EXPR_TRUE);
      } else if (booleanExpr instanceof ConstantBooleanExpr) {
         treeCounters.add(((ConstantBooleanExpr) booleanExpr).getValue() ? BETree.EXPR_TRUE : BETree.EXPR_FALSE);
      } else {
         preorderTraversal(null, booleanExpr, nodes, treeCounters, namedParameters);
      }

      int[] countersArray = new int[treeCounters.size()];
      for (int i = 0; i < countersArray.length; i++) {
         countersArray[i] = treeCounters.get(i);
      }
      return new BETree(nodes.toArray(new BENode[nodes.size()]), countersArray);
   }

   private void preorderTraversal(BENode parent, BooleanExpr child, List<BENode> nodes, List<Integer> treeCounters, Map<String, Object> namedParameters) {
      if (child instanceof NotExpr) {
         PrimaryPredicateExpr condition = (PrimaryPredicateExpr) ((NotExpr) child).getChild();
         makePredicateNode(parent, nodes, treeCounters, condition, true, namedParameters);
      } else if (child instanceof PrimaryPredicateExpr) {
         PrimaryPredicateExpr condition = (PrimaryPredicateExpr) child;
         makePredicateNode(parent, nodes, treeCounters, condition, false, namedParameters);
      } else if (child instanceof OrExpr) {
         makeBooleanOperatorNode((OrExpr) child, nodes, treeCounters, new OrNode(parent), namedParameters);
      } else if (child instanceof AndExpr) {
         makeBooleanOperatorNode((AndExpr) child, nodes, treeCounters, new AndNode(parent), namedParameters);
      } else {
         throw new IllegalStateException("Unexpected *Expr node type: " + child);
      }
   }

   private void makePredicateNode(BENode parent, List<BENode> nodes, List<Integer> treeCounters, PrimaryPredicateExpr condition, boolean isNegated, Map<String, Object> namedParameters) {
      final PropertyValueExpr pve = (PropertyValueExpr) condition.getChild();
      final List<AttributeId> path = metadataAdapter.translatePropertyPath(pve.getPropertyPath());
      final boolean isRepeated = pve.isRepeated();

      if (condition instanceof ComparisonExpr) {
         ComparisonExpr expr = (ComparisonExpr) condition;
         ConstantValueExpr right = (ConstantValueExpr) expr.getRightChild();
         Comparable rightConstant = right.getConstantValueAs(pve.getPrimitiveType(), namedParameters);
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
                  addPredicateNode(parent, nodes, treeCounters, isNegated, path, new IntervalPredicate(isRepeated, new Interval(Interval.getMinusInf(), false, rightConstant, false)));
                  addPredicateNode(parent, nodes, treeCounters, isNegated, path, new IntervalPredicate(isRepeated, new Interval(rightConstant, false, Interval.getPlusInf(), false)));
                  addPredicateNode(parent, nodes, treeCounters, isNegated, path, new Predicate<Object>(isRepeated, IsNullCondition.INSTANCE));
               } else {
                  addPredicateNode(parent, nodes, treeCounters, !isNegated, path, new Predicate<Object>(isRepeated, new EqualsCondition(rightConstant)));
               }
               break;
            case EQUAL:
               if (useIntervals) {
                  addPredicateNode(parent, nodes, treeCounters, isNegated, path, new IntervalPredicate(isRepeated, new Interval(rightConstant, true, rightConstant, true)));
               } else {
                  addPredicateNode(parent, nodes, treeCounters, isNegated, path, new Predicate<>(isRepeated, new EqualsCondition(rightConstant)));
               }
               break;
            case LESS:
               addPredicateNode(parent, nodes, treeCounters, isNegated, path, new IntervalPredicate(isRepeated, new Interval(Interval.getMinusInf(), false, rightConstant, false)));
               break;
            case LESS_OR_EQUAL:
               addPredicateNode(parent, nodes, treeCounters, isNegated, path, new IntervalPredicate(isRepeated, new Interval(Interval.getMinusInf(), false, rightConstant, true)));
               break;
            case GREATER:
               addPredicateNode(parent, nodes, treeCounters, isNegated, path, new IntervalPredicate(isRepeated, new Interval(rightConstant, false, Interval.getPlusInf(), false)));
               break;
            case GREATER_OR_EQUAL:
               addPredicateNode(parent, nodes, treeCounters, isNegated, path, new IntervalPredicate(isRepeated, new Interval(rightConstant, true, Interval.getPlusInf(), false)));
               break;
            default:
               throw new IllegalStateException("Unexpected comparison type: " + expr.getComparisonType());
         }
      } else if (condition instanceof IsNullExpr) {
         addPredicateNode(parent, nodes, treeCounters, isNegated, path, new Predicate<>(isRepeated, IsNullCondition.INSTANCE));
      } else if (condition instanceof LikeExpr) {
         addPredicateNode(parent, nodes, treeCounters, isNegated, path, new Predicate<>(isRepeated, new RegexCondition(((LikeExpr) condition).getPattern())));
      } else {
         throw new IllegalStateException("Unexpected condition type: " + condition);
      }
   }

   private void addPredicateNode(BENode parent, List<BENode> nodes, List<Integer> treeCounters, boolean isNegated, List<AttributeId> path, Predicate predicate) {
      PredicateNode predicateNode = new PredicateNode<>(parent, predicate, isNegated, path);
      int size = nodes.size();
      predicateNode.setLocation(size, size + 1);
      nodes.add(predicateNode);
      treeCounters.add(1);
   }

   private void makeBooleanOperatorNode(BooleanOperatorExpr child, List<BENode> nodes, List<Integer> treeCounters, BENode node, Map<String, Object> namedParameters) {
      int index = nodes.size();
      nodes.add(node);
      List<BooleanExpr> children = child.getChildren();
      treeCounters.add(children.size());
      for (BooleanExpr c : children) {
         preorderTraversal(node, c, nodes, treeCounters, namedParameters);
      }
      node.setLocation(index, nodes.size());
   }
}
