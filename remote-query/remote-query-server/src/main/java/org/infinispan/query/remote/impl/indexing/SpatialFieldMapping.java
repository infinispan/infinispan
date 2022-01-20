package org.infinispan.query.remote.impl.indexing;

/**
 * @author anistor@redhat.com
 * @since 14
 */
public final class SpatialFieldMapping {

   private final String fieldName;

   private final String markerSet;

   private final boolean store;

   private String latitude;

   private String longitude;

   SpatialFieldMapping(String fieldName, String markerSet, boolean store) {
      this.fieldName = fieldName;
      this.markerSet = markerSet;
      this.store = store;
   }

   public String name() {
      return fieldName;
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
      return "SpatialFieldMapping{" +
            "fieldName='" + fieldName + '\'' +
            "markerSet='" + markerSet + '\'' +
            "store=" + store +
            "latitude=" + latitude +
            "longitude=" + longitude +
            '}';
   }
}
