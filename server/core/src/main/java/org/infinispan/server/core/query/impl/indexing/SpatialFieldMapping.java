package org.infinispan.server.core.query.impl.indexing;

import org.infinispan.protostream.descriptors.FieldDescriptor;

/**
 * @author anistor@redhat.com
 * @since 15
 */
public final class SpatialFieldMapping {

   private final String fieldName;

   private final String marker;

   private final boolean projectable;

   private final boolean sortable;

   private FieldDescriptor latitude;

   private FieldDescriptor longitude;

   public SpatialFieldMapping(String fieldName, String marker, boolean projectable, boolean sortable) {
      this.fieldName = fieldName;
      this.marker = marker;
      this.projectable = projectable;
      this.sortable = sortable;
   }

   public String fieldName() {
      return fieldName;
   }

   public String marker() {
      return marker;
   }

   public boolean projectable() {
      return projectable;
   }

   public boolean sortable() {
      return sortable;
   }

   public FieldDescriptor getLatitude() {
      return latitude;
   }

   public void setLatitude(FieldDescriptor latitude) {
      this.latitude = latitude;
   }

   public FieldDescriptor getLongitude() {
      return longitude;
   }

   public void setLongitude(FieldDescriptor longitude) {
      this.longitude = longitude;
   }

   @Override
   public String toString() {
      return "SpatialFieldMapping{" +
            "fieldName='" + fieldName + '\'' +
            "marker='" + marker + '\'' +
            "projectable=" + projectable +
            "sortable=" + sortable +
            "latitude=" + latitude +
            "longitude=" + longitude +
            '}';
   }
}
