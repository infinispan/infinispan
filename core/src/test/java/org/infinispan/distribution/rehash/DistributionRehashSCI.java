package org.infinispan.distribution.rehash;

import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.ProtoSchema;
import org.infinispan.protostream.annotations.ProtoSyntax;

@ProtoSchema(
      includeClasses = {
            NonTxBackupOwnerBecomingPrimaryOwnerTest.CustomConsistentHashFactory.class,
            NonTxPrimaryOwnerBecomingNonOwnerTest.CustomConsistentHashFactory.class,
      },
      schemaFileName = "test.core.distribution.rehash.proto",
      schemaFilePath = "proto/generated",
      schemaPackageName = "org.infinispan.test.core.distribution.rehash",
      service = false,
      syntax = ProtoSyntax.PROTO3
)
public interface DistributionRehashSCI extends SerializationContextInitializer {
   DistributionRehashSCI INSTANCE = new DistributionRehashSCIImpl();
}
