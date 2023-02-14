package org.infinispan.client.hotrod.query.testdomain.protobuf;

import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;

public class LongAuto {

   private Long purchases;

   @ProtoFactory
   public LongAuto(Long purchases) {
      this.purchases = purchases;
   }

   @ProtoField(value = 1)
   public Long getPurchases() {
      return purchases;
   }

   @AutoProtoSchemaBuilder(includeClasses = LongAuto.class,
         schemaFilePath = "/protostream", schemaFileName = "long-auto.proto",
         schemaPackageName = "lab.auto")
   public interface LongSchema extends GeneratedSchema {
   }
}
