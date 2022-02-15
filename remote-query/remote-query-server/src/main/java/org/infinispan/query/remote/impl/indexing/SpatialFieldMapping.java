package org.infinispan.query.remote.impl.indexing;

/**
 * @author anistor@redhat.com
 * @since 14
 */
public final class SpatialFieldMapping {

   private final String fieldName;

   private final String markerSet;

   private final boolean projectable;

   private final boolean sortable;

   private String latitude;

   private String longitude;

   SpatialFieldMapping(String fieldName, String markerSet, boolean projectable, boolean sortable) {
      this.fieldName = fieldName;
      this.markerSet = markerSet;
      this.projectable = projectable;
      this.sortable = sortable;
   }

   public String fieldName() {
      return fieldName;
   }

   public String markerSet() {
      return markerSet;
   }

   public boolean projectable() {
      return projectable;
   }

   public boolean sortable() {
      return sortable;
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
            "projectable=" + projectable +
            "sortable=" + sortable +
            "latitude=" + latitude +
            "longitude=" + longitude +
            '}';
   }
}
