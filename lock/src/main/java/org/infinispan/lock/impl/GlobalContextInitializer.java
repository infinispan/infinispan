package org.infinispan.lock.impl;

import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.ProtoSchema;
import org.infinispan.protostream.annotations.ProtoSyntax;

@ProtoSchema(
      dependsOn = org.infinispan.marshall.persistence.impl.PersistenceContextInitializer.class,
      includeClasses = {
            org.infinispan.lock.impl.entries.ClusteredLockKey.class,
            org.infinispan.lock.impl.entries.ClusteredLockValue.class,
            org.infinispan.lock.impl.entries.ClusteredLockState.class,
            org.infinispan.lock.impl.functions.IsLocked.class,
            org.infinispan.lock.impl.functions.LockFunction.class,
            org.infinispan.lock.impl.functions.UnlockFunction.class,
            org.infinispan.lock.impl.lock.ClusteredLockFilter.class
      },
      schemaFileName = "global.lock.proto",
      schemaFilePath = "proto/generated",
      schemaPackageName = "org.infinispan.global.lock",
      service = false,
      syntax = ProtoSyntax.PROTO3
)
interface GlobalContextInitializer extends SerializationContextInitializer {
}
