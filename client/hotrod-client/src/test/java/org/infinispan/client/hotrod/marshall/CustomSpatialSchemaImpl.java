package org.infinispan.client.hotrod.marshall;

import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.protostream.SerializationContext;

public class CustomSpatialSchemaImpl extends SpatialSchemaImpl implements GeneratedSchema {

   private static final String PROTO_SCHEMA = """
      syntax = "proto2";
      package sample_bank_account;
      /**
       * @Indexed
       * @GeoPoint(fieldName = "start", marker = "start", projectable = true, sortable = true)
       * @GeoPoint(fieldName = "end", marker = "end", projectable = true, sortable = true)
       */
      message FlightRoute {
         /**
          * @Basic
          */
         optional string name = 1;
         /**
          * @Latitude(marker = "start")
          */
         optional double startLat = 2;
         /**
          * @Longitude(marker = "start")
          */
         optional double startLon = 3;
         /**
          * @Latitude(marker = "end")
          */
         optional double endLat = 4;
         /**
          * @Longitude(marker = "end")
          */
         optional double endLon = 5;
      }
   """;

   @Override
   public String getProtoFile() {
      return PROTO_SCHEMA;
   }

   @Override
   public void registerSchema(SerializationContext serCtx) {
      serCtx.registerProtoFiles(org.infinispan.protostream.FileDescriptorSource.fromString(getProtoFileName(), getProtoFile()));
   }
}
