package org.infinispan.client.hotrod.query.testdomain.protobuf;

import java.math.BigInteger;

import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.types.java.CommonTypes;

public class CalculusAuto {

   private BigInteger purchases;

   @ProtoFactory
   public CalculusAuto(BigInteger purchases) {
      this.purchases = purchases;
   }

   @ProtoField(value = 1)
   public BigInteger getPurchases() {
      return purchases;
   }

   @AutoProtoSchemaBuilder(includeClasses = CalculusAuto.class, dependsOn = CommonTypes.class,
         schemaFilePath = "/protostream", schemaFileName = "calculus-auto.proto",
         schemaPackageName = "lab.auto")
   public interface CalculusAutoSchema extends GeneratedSchema {
   }
}
