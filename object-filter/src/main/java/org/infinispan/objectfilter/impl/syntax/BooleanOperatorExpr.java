package org.infinispan.objectfilter.impl.syntax;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * An expression that applies a boolean operator (OR, AND) to a list of boolean sub-expressions.
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
public abstract class BooleanOperatorExpr implements BooleanExpr {

   protected final List<BooleanExpr> children = new ArrayList<>();

   protected BooleanOperatorExpr(BooleanExpr... children) {
      Collections.addAll(this.children, children);
   }

   protected BooleanOperatorExpr(List<BooleanExpr> children) {
      this.children.addAll(children);
   }

   public List<BooleanExpr> getChildren() {
      return children;
   }
}
