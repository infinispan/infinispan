package org.infinispan.objectfilter.impl.syntax;

/**
 * @author anistor@redhat.com
 * @since 9.4
 */
public final class GeofiltExpr implements PrimaryPredicateExpr {

   // coordinates field
   private final PropertyValueExpr propertyValueExpr;

   private final double lat;

   private final double lon;

   private final double radius;

   public GeofiltExpr(PropertyValueExpr propertyValueExpr, double lat, double lon, double radius) {
      this.propertyValueExpr = propertyValueExpr;
      this.lat = lat;
      this.lon = lon;
      this.radius = radius;
   }

   public double getLatitude() {
      return lat;
   }

   public double getLongitude() {
      return lon;
   }

   public double getRadius() {
      return radius;
   }

   @Override
   public String toQueryString() {
      return "geofilt(" + propertyValueExpr + ", " + lat + ", " + lon + ", " + radius + ')';
   }

   @Override
   public <T> T acceptVisitor(Visitor<?, ?> visitor) {
      return (T) visitor.visit(this);
   }

   @Override
   public PropertyValueExpr getChild() {
      return propertyValueExpr;
   }
}
