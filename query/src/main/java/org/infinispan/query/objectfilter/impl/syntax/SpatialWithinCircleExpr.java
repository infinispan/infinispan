package org.infinispan.query.objectfilter.impl.syntax;

import java.util.Objects;

/**
 * @author anistor@redhat.com
 * @since 14.0
 */
public final class SpatialWithinCircleExpr implements PrimaryPredicateExpr {

   public static final String DEFAULT_UNIT = "m";

   private final ValueExpr leftChild;

   private final ValueExpr latChild;

   private final ValueExpr lonChild;

   private final ValueExpr radiusChild;

   private final ConstantValueExpr unitChild;

   public SpatialWithinCircleExpr(ValueExpr leftChild, ValueExpr latChild, ValueExpr lonChild,
                                  ValueExpr radiusChild, ConstantValueExpr unitChild) {
      this.leftChild = leftChild;
      this.latChild = latChild;
      this.lonChild = lonChild;
      this.radiusChild = radiusChild;
      this.unitChild = unitChild;
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

   public ConstantValueExpr getUnitChild() {
      return unitChild;
   }

   @Override
   public <T> T acceptVisitor(Visitor<?, ?> visitor) {
      return (T) visitor.visit(this);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      SpatialWithinCircleExpr that = (SpatialWithinCircleExpr) o;
      return Objects.equals(leftChild, that.leftChild) && Objects.equals(latChild, that.latChild)
            && Objects.equals(lonChild, that.lonChild) && Objects.equals(radiusChild, that.radiusChild)
            && Objects.equals(unitChild, that.unitChild);
   }

   @Override
   public int hashCode() {
      return Objects.hash(leftChild, latChild, lonChild, radiusChild, unitChild);
   }

   @Override
   public String toString() {
      return "WITHIN_CIRCLE(" + leftChild + ", " + latChild + ", " + lonChild + ", " + radiusChild + ", " + unitChild + ")";
   }

   @Override
   public void appendQueryString(StringBuilder sb) {
      leftChild.appendQueryString(sb);
      sb.append(" WITHIN CIRCLE( ");
      latChild.appendQueryString(sb);
      sb.append(", ");
      lonChild.appendQueryString(sb);
      sb.append(", ");
      radiusChild.appendQueryString(sb);
      sb.append(unitChild.getConstantValue());
      sb.append(" )");
   }
}
