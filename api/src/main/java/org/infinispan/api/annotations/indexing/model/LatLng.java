package org.infinispan.api.annotations.indexing.model;

import org.hibernate.search.engine.spatial.GeoPoint;
import org.infinispan.api.annotations.indexing.Latitude;
import org.infinispan.api.annotations.indexing.Longitude;
import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.protostream.annotations.Proto;
import org.infinispan.protostream.annotations.ProtoSchema;
import org.infinispan.protostream.annotations.ProtoSyntax;

/**
 * A point in the geocentric coordinate system.
 * <p>
 * Simplified version for Infinispan of {@link GeoPoint}
 *
 * @since 15.1
 */
@Proto
public record LatLng(@Latitude double latitude, @Longitude double longitude) {

   public static LatLng of(double latitude, double longitude) {
      return new LatLng(latitude, longitude);
   }

   @ProtoSchema(
         includeClasses = {LatLng.class},
         schemaFileName = "latlng.proto",
         schemaPackageName = "google.type",
         syntax = ProtoSyntax.PROTO3
   )
   public interface LatLngSchema extends GeneratedSchema {
   }
}
