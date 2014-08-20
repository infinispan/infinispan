package org.infinispan.objectfilter.impl.predicateindex.be;

import org.infinispan.objectfilter.impl.MetadataAdapter;
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

   private final MetadataAdapter<?, AttributeId> attributePathTranslator;

   public BETreeMaker(MetadataAdapter<?, AttributeId> attributePathTranslator) {
      this.attributePathTranslator = attributePathTranslator;
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
      Predicate predicate = makePredicate(condition);
      List<String> propertyPath = ((PropertyValueExpr) condition.getChild()).getPropertyPath();
      List<AttributeId> translatedPath = attributePathTranslator.translatePropertyPath(propertyPath);
      boolean isRepeated = attributePathTranslator.isRepeatedProperty(propertyPath);
      PredicateNode node = new PredicateNode<AttributeId>(parent, predicate, isNegated, translatedPath, isRepeated);
      int size = nodes.size();
      node.setLocation(size, size);
      nodes.add(node);
      treeCounters.add(1);
   }

   private Predicate makePredicate(PrimaryPredicateExpr condition) {
      if (condition instanceof ComparisonExpr) {
         ComparisonExpr expr = (ComparisonExpr) condition;
         ConstantValueExpr right = (ConstantValueExpr) expr.getRightChild();
         Interval<Object> i;
         switch (expr.getComparisonType()) {
            case EQUALS:
               i = new Interval<Object>(right.getConstantValue(), true, right.getConstantValue(), true);
               break;
            case LESS:
               i = new Interval<Object>(Interval.getMinusInf(), false, right.getConstantValue(), false);
               break;
            case LESS_OR_EQUAL:
               i = new Interval<Object>(Interval.getMinusInf(), false, right.getConstantValue(), true);
               break;
            case GREATER:
               i = new Interval<Object>(right.getConstantValue(), false, Interval.getPlusInf(), false);
               break;
            case GREATER_OR_EQUAL:
               i = new Interval<Object>(right.getConstantValue(), true, Interval.getPlusInf(), false);
               break;
            default:
               throw new IllegalStateException("Unknown comparison type: " + expr.getComparisonType());
         }
         return new Predicate<Object>(i);
      } else if (condition instanceof IsNullExpr) {
         return new Predicate<Object>(IsNullCondition.INSTANCE);
      } else if (condition instanceof RegexExpr) {
         return new Predicate<String>(new RegexCondition(((RegexExpr) condition).getPattern()));
      }

      throw new IllegalStateException("Unexpected condition type: " + condition);
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
