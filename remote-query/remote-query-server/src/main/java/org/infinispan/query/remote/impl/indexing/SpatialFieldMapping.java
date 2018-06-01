package org.infinispan.query.remote.impl.indexing;

import org.hibernate.search.annotations.SpatialMode;

/**
 * @author anistor@redhat.com
 * @since 9.4
 */
public final class SpatialFieldMapping {

   private final String name;

   private final boolean store;

   private final SpatialMode spatialMode;

   private String latitude;

   private String longitude;

   SpatialFieldMapping(String name, SpatialMode spatialMode, boolean store) {
      this.name = name;
      this.spatialMode = spatialMode;
      this.store = store;
   }

   public String name() {
      return name;
   }

   public boolean store() {
      return store;
   }

   public String latitude() {
      return latitude;
   }

   void setLatitude(String latitude) {
      this.latitude = latitude;
   }

   public String longitude() {
      return longitude;
   }

   void setLongitude(String longitude) {
      this.longitude = longitude;
   }

   @Override
   public String toString() {
      return "SpatialMapping{" +
            "name='" + name + '\'' +
            "spatialMode=" + spatialMode +
            "store=" + store +
            "latitude=" + latitude +
            "longitude=" + longitude +
            '}';
   }
}
