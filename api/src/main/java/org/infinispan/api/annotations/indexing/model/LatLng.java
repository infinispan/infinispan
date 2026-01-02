package org.infinispan.api.annotations.indexing.model;

import org.infinispan.api.annotations.indexing.Latitude;
import org.infinispan.api.annotations.indexing.Longitude;

/**
 * A point in the geocentric coordinate system.
 * <p>
 * Simplified version for Infinispan of {@link org.hibernate.search.engine.spatial.GeoPoint}
 *
 * @since 15.1
 */
public final class LatLng {

   public static LatLng of(double latitude, double longitude) {
      return new LatLng(latitude, longitude);
   }

   @Longitude
   private final double longitude;

   @Latitude
   private final double latitude;

   public LatLng(double latitude, double longitude) {
      this.latitude = latitude;
      this.longitude = longitude;
   }

   /**
    * @return the latitude, in degrees
    */
   public double latitude() {
      return latitude;
   }

   /**
    * @return the longitude, in degrees
    */
   public double longitude() {
      return longitude;
   }
}
