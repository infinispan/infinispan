package org.infinispan.client.hotrod.query.testdomain.protobuf;

import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.protostream.annotations.Proto;
import org.infinispan.protostream.annotations.ProtoSchema;

@Proto
public record LongAuto(Long purchases) {

   @ProtoSchema(
         includeClasses = LongAuto.class,
         schemaFilePath = "/protostream",
         schemaFileName = "long-auto.proto",
         schemaPackageName = "lab.auto",
         service = false
   )
   public interface LongSchema extends GeneratedSchema {
   }
}
