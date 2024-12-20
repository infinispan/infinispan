package org.infinispan.counter.impl;

import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.ProtoSchema;
import org.infinispan.protostream.annotations.ProtoSyntax;

@ProtoSchema(
      allowNullFields = true,
      dependsOn = {
            org.infinispan.counter.api._private.PersistenceContextInitializer.class,
            org.infinispan.counter.impl.persistence.PersistenceContextInitializer.class
      },
      includeClasses = {
            org.infinispan.counter.impl.function.AddFunction.class,
            org.infinispan.counter.impl.function.CompareAndSwapFunction.class,
            org.infinispan.counter.impl.function.CreateAndAddFunction.class,
            org.infinispan.counter.impl.function.CreateAndCASFunction.class,
            org.infinispan.counter.impl.function.CreateAndSetFunction.class,
            org.infinispan.counter.impl.function.InitializeCounterFunction.class,
            org.infinispan.counter.impl.function.ReadFunction.class,
            org.infinispan.counter.impl.function.RemoveFunction.class,
            org.infinispan.counter.impl.function.ResetFunction.class,
            org.infinispan.counter.impl.function.SetFunction.class
      },
      schemaFileName = "global.counters.proto",
      schemaFilePath = "proto/generated",
      schemaPackageName = "org.infinispan.global.counters",
      service = false,
      syntax = ProtoSyntax.PROTO3
)
interface GlobalContextInitializer extends SerializationContextInitializer {
}
