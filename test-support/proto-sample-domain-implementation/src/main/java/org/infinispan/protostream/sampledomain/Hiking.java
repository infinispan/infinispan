package org.infinispan.protostream.sampledomain;

import java.util.LinkedHashMap;
import java.util.Map;

import org.infinispan.api.annotations.indexing.GeoField;
import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.api.annotations.indexing.Keyword;
import org.infinispan.api.annotations.indexing.model.LatLng;
import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.protostream.annotations.Proto;
import org.infinispan.protostream.annotations.ProtoSchema;
import org.infinispan.protostream.annotations.ProtoSyntax;

@Proto
@Indexed
public record Hiking(@Keyword(projectable = true) String name,
                     @GeoField(projectable = true, sortable = true) LatLng start,
                     @GeoField(projectable = true, sortable = true) LatLng end) {

   public static Map<String, Hiking> data() {
      Map<String, Hiking> data = new LinkedHashMap<>();
      data.put("track 1", new Hiking("track 1", LatLng.of(41.907903484609356, 12.45540543756422),
            LatLng.of(41.90369455835456, 12.459566517195528)));
      data.put("track 2", new Hiking("track 2", LatLng.of(41.90369455835456, 12.459566517195528),
            LatLng.of(41.907930453801285, 12.455204785977637)));
      data.put("track 3", new Hiking("track 3", LatLng.of(41.907930453801285, 12.455204785977637),
            LatLng.of(41.907903484609356, 12.45540543756422)));
      return data;
   }

   @ProtoSchema(
         dependsOn = LatLng.LatLngSchema.class,
         includeClasses = Hiking.class,
         schemaFileName = "hiking.proto",
         schemaPackageName = "geo",
         syntax = ProtoSyntax.PROTO3,
         service = false
   )
   public interface HikingSchema extends GeneratedSchema {
      Hiking.HikingSchema INSTANCE = new HikingSchemaImpl();
   }
}
