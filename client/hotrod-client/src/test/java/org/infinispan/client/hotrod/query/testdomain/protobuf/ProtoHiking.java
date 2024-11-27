package org.infinispan.client.hotrod.query.testdomain.protobuf;

import org.infinispan.api.annotations.indexing.GeoField;
import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.api.annotations.indexing.Keyword;
import org.infinispan.commons.api.query.geo.LatLng;
import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.protostream.annotations.Proto;
import org.infinispan.protostream.annotations.ProtoSchema;
import org.infinispan.protostream.annotations.ProtoSyntax;

@Proto
@Indexed
public record ProtoHiking(@Keyword(projectable = true) String name,
                          @GeoField(projectable = true, sortable = true) LatLng start,
                          @GeoField(projectable = true, sortable = true) LatLng end) {

   @ProtoSchema(
         dependsOn = LatLng.LatLngSchema.class,
         includeClasses = ProtoHiking.class,
         schemaFileName = "hiking.proto",
         schemaPackageName = "geo",
         syntax = ProtoSyntax.PROTO3,
         service = false
   )
   public interface ProtoHikingSchema extends GeneratedSchema {
      ProtoHikingSchema INSTANCE = new ProtoHikingSchemaImpl();
   }
}
