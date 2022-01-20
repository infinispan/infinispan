package org.infinispan.objectfilter.impl.syntax;

/**
 * @author anistor@redhat.com
 * @since 14.0
 */
public final class SpatialWithinCircleExpr implements PrimaryPredicateExpr {

   private final ValueExpr leftChild;

   private final ValueExpr latChild;

   private final ValueExpr lonChild;

   private final ValueExpr radiusChild;

   public SpatialWithinCircleExpr(ValueExpr leftChild, ValueExpr latChild, ValueExpr lonChild, ValueExpr radiusChild) {
      this.leftChild = leftChild;
      this.latChild = latChild;
      this.lonChild = lonChild;
      this.radiusChild = radiusChild;
   }

   @Override
   public ValueExpr getChild() {
      return leftChild;
   }

   public ValueExpr getLeftChild() {
      return leftChild;
   }

   public ValueExpr getLatChild() {
      return latChild;
   }

   public ValueExpr getLonChild() {
      return lonChild;
   }

   public ValueExpr getRadiusChild() {
      return radiusChild;
   }

   @Override
   public <T> T acceptVisitor(Visitor<?, ?> visitor) {
      return (T) visitor.visit(this);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      SpatialWithinCircleExpr other = (SpatialWithinCircleExpr) o;
      return leftChild.equals(other.leftChild)
            && latChild.equals(other.latChild)
            && lonChild.equals(other.lonChild)
            && radiusChild.equals(other.radiusChild);
   }

   @Override
   public int hashCode() {
      return 31 * (31 * (31 * leftChild.hashCode() + latChild.hashCode()) + lonChild.hashCode()) + radiusChild.hashCode();
   }

   @Override
   public String toString() {
      return "WITHIN_CIRCLE(" + leftChild + ", " + latChild + ", " + lonChild + ", " + radiusChild + ")";
   }

   @Override
   public String toQueryString() {
      return leftChild.toQueryString() + " WITHIN CIRCLE( "
            + latChild.toQueryString() + ", " + lonChild.toQueryString()
            + ", " + radiusChild.toQueryString() + ")";
   }
}
