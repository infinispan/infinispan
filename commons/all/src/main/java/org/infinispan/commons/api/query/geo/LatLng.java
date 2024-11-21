package org.infinispan.commons.api.query.geo;

import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.protostream.annotations.Proto;
import org.infinispan.protostream.annotations.ProtoSchema;
import org.infinispan.protostream.annotations.ProtoSyntax;

/**
 * A point in the geocentric coordinate system.
 * Providing a Proto Schema compatible with Google API latlng.proto.
 *
 * @see <a href="https://github.com/googleapis">Google API</a>
 * @since 15.1
 */
@Proto
public record LatLng(double latitude, double longitude) {

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
