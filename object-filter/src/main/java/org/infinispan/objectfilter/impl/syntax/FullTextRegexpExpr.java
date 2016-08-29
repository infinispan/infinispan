package org.infinispan.objectfilter.impl.syntax;

/**
 * @author anistor@redhat.com
 * @since 9.0
 */
public final class FullTextRegexpExpr implements PrimaryPredicateExpr {

   private final ValueExpr leftChild;

   private final String regexp;

   public FullTextRegexpExpr(ValueExpr leftChild, String regexp) {
      this.leftChild = leftChild;
      this.regexp = regexp;
   }

   public String getRegexp() {
      return regexp;
   }

   @Override
   public <T> T acceptVisitor(Visitor<?, ?> visitor) {
      return (T) visitor.visit(this);
   }

   @Override
   public ValueExpr getChild() {
      return leftChild;
   }

   @Override
   public String toString() {
      return leftChild.toString() + ":/" + regexp + "/";
   }

   @Override
   public String toQueryString() {
      return leftChild.toQueryString() + ":/" + regexp + "/";
   }
}
