package org.infinispan.counter.api._private;

import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterState;
import org.infinispan.counter.api.CounterType;
import org.infinispan.counter.api.Storage;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.ProtoSchema;

/**
 * @since 15.1
 */
@ProtoSchema(
      includeClasses = {
            CounterState.class,
            CounterConfiguration.class,
            CounterType.class,
            Storage.class
      },
      schemaFileName = "persistence.counters-api.proto",
      schemaFilePath = "org/infinispan/counter/api",
      schemaPackageName = "org.infinispan.persistence.commons", // to preserve backwards compatibility
      service = false,
      orderedMarshallers = true
)
public interface PersistenceContextInitializer extends SerializationContextInitializer {
}
