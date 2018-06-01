package org.infinispan.objectfilter.impl.syntax.parser;

import java.util.List;

import org.infinispan.objectfilter.impl.ql.PropertyPath;

/**
 * @author anistor@redhat.com
 * @since 9.4
 */
public final class GeoDistPropertyPath<TypeMetadata> extends PropertyPath<TypeDescriptor<TypeMetadata>> {

   // The pin
   private Double lat;

   private Double lon;

   GeoDistPropertyPath(List<PropertyReference<TypeDescriptor<TypeMetadata>>> path) {
      super(path);
   }

   GeoDistPropertyPath(List<PropertyReference<TypeDescriptor<TypeMetadata>>> path, Double lat, Double lon) {
      this(path);
      this.lat = lat;
      this.lon = lon;
   }

   public Double getLatitude() {
      return lat;
   }

   public void setLatitude(Double lat) {
      this.lat = lat;
   }

   public Double getLongitude() {
      return lon;
   }

   public void setLongitude(Double lon) {
      this.lon = lon;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof GeoDistPropertyPath)) return false;
      if (!super.equals(o)) return false;
      GeoDistPropertyPath<?> that = (GeoDistPropertyPath<?>) o;
      if (lat != null ? !lat.equals(that.lat) : that.lat != null) return false;
      return lon != null ? lon.equals(that.lon) : that.lon == null;
   }

   @Override
   public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + (lat != null ? lat.hashCode() : 0);
      result = 31 * result + (lon != null ? lon.hashCode() : 0);
      return result;
   }

   @Override
   public String toString() {
      return "geodist(" + asStringPath() + ", " + lat + ", " + lon + ')';
   }
}
