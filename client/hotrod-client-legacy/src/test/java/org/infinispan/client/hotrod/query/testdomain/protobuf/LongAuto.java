package org.infinispan.client.hotrod.query.testdomain.protobuf;

import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoSchema;

public class LongAuto {

   private final Long purchases;

   @ProtoFactory
   public LongAuto(Long purchases) {
      this.purchases = purchases;
   }

   @ProtoField(value = 1)
   public Long getPurchases() {
      return purchases;
   }

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
