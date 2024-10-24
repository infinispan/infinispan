package org.infinispan.client.hotrod.marshall;

import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.protostream.SerializationContext;

public class CustomSpacialSchemaImpl extends SpatialSchemaImpl implements GeneratedSchema {

   private static final String PROTO_SCHEMA = "// File name: spatial.proto\n" +
         "// Generated from : org.infinispan.client.hotrod.marshall.SpatialSchema\n" +
         "\n" +
         "syntax = \"proto2\";\n" +
         "\n" +
         "package sample_bank_account;\n" +
         "\n" +
         "\n" +
         "\n" +
         "/**\n" +
         " * @Indexed\n" +
         " * @GeoPointBinding(fieldName = \"start\", markerSet = \"start\", projectable = Projectable.YES, sortable = Sortable.YES)\n" +
         " * @GeoPointBinding(fieldName = \"end\", markerSet = \"end\", projectable = Projectable.YES, sortable = Sortable.YES)\n" +
         " */\n" +
         "message FlightRoute {\n" +
         "   \n" +
         "   /**\n" +
         "    * @Field(store = Store.YES)\n" +
         "    */\n" +
         "   optional string name = 1;\n" +
         "   \n" +
         "   /**\n" +
         "    * @Latitude(markerSet = \"start\")\n" +
         "    */\n" +
         "   optional double startLat = 2;\n" +
         "   \n" +
         "   /**\n" +
         "    * @Longitude(markerSet = \"start\")\n" +
         "    */\n" +
         "   optional double startLon = 3;\n" +
         "   \n" +
         "   /**\n" +
         "    * @Latitude(markerSet = \"end\")\n" +
         "    */\n" +
         "   optional double endLat = 4;\n" +
         "   \n" +
         "   /**\n" +
         "    * @Longitude(markerSet = \"end\")\n" +
         "    */\n" +
         "   optional double endLon = 5;\n" +
         "}\n" +
         "";

   @Override
   public String getProtoFile() {
      return PROTO_SCHEMA;
   }

   @Override
   public void registerSchema(SerializationContext serCtx) {
      serCtx.registerProtoFiles(org.infinispan.protostream.FileDescriptorSource.fromString(getProtoFileName(), getProtoFile()));
   }
}
