package org.infinispan.commons.marshall;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.util.KeyValueWithPrevious;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterState;
import org.infinispan.counter.api.CounterType;
import org.infinispan.counter.api.Storage;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;

/**
 * Interface used to initialise a {@link org.infinispan.protostream.SerializationContext} using the specified Pojos,
 * Marshaller implementations and provided .proto schemas.
 *
 * @author Ryan Emerson
 * @since 10.0
 */
@AutoProtoSchemaBuilder(
      includeClasses = {
            KeyValueWithPrevious.class,
            MediaType.class,
            WrappedByteArray.class,
            CounterState.class,
            CounterConfiguration.class,
            CounterType.class,
            Storage.class,
      },
      schemaFileName = "persistence.commons.proto",
      schemaFilePath = "proto/generated",
      schemaPackageName = "org.infinispan.persistence.commons")
public interface PersistenceContextInitializer extends SerializationContextInitializer {
}
