package org.infinispan.commons;

import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.ProtoSchema;
import org.infinispan.protostream.annotations.ProtoSyntax;
import org.infinispan.protostream.types.java.CommonTypes;

/**
 * Interface used to initialise the global {@link org.infinispan.protostream.SerializationContext} using the specified Pojos,
 * and the generated proto files and marshallers.
 *
 * @author Ryan Emerson
 * @since 10.0
 */
@ProtoSchema(
      allowNullFields = true,
      dependsOn = CommonTypes.class,
      includeClasses = {
            org.infinispan.commons.api.CacheContainerAdmin.AdminFlag.class,
            org.infinispan.commons.marshall.protostream.adapters.AtomicIntegerArrayAdapter.class,
            org.infinispan.commons.tx.XidImpl.class,
            org.infinispan.commons.util.ConcurrentSmallIntSet.class,
            org.infinispan.commons.util.EmptyIntSet.class,
            org.infinispan.commons.util.RangeSet.class,
            org.infinispan.commons.util.SingletonIntSet.class,
            org.infinispan.commons.util.SmallIntSet.class,
            org.infinispan.commons.util.KeyValueWithPrevious.class,
      },
      schemaFileName = "global.commons.proto",
      schemaFilePath = "proto/generated",
      schemaPackageName = "org.infinispan.global.commons",
      service = false,
      syntax = ProtoSyntax.PROTO3
)
public interface GlobalContextInitializer extends SerializationContextInitializer {
   GlobalContextInitializer INSTANCE = new GlobalContextInitializerImpl();
}
