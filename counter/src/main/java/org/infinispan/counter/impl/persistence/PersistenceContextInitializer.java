package org.infinispan.counter.impl.persistence;

import org.infinispan.counter.api.CounterState;
import org.infinispan.counter.impl.entries.CounterValue;
import org.infinispan.counter.impl.strong.StrongCounterKey;
import org.infinispan.counter.impl.weak.WeakCounterKey;
import org.infinispan.marshall.persistence.impl.PersistenceMarshallerImpl;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;

/**
 * Interface used to initialise the {@link PersistenceMarshallerImpl}'s {@link org.infinispan.protostream.SerializationContext}
 * using the specified Pojos, Marshaller implementations and provided .proto schemas.
 *
 * @author Ryan Emerson
 * @since 10.0
 */
@AutoProtoSchemaBuilder(
      dependsOn = org.infinispan.marshall.persistence.impl.PersistenceContextInitializer.class,
      includeClasses = {
            CounterState.class,
            CounterValue.class,
            StrongCounterKey.class,
            WeakCounterKey.class
      },
      schemaFileName = "persistence.counters.proto",
      schemaFilePath = "proto/generated",
      schemaPackageName = "org.infinispan.persistence.counters")
interface PersistenceContextInitializer extends SerializationContextInitializer {
}
