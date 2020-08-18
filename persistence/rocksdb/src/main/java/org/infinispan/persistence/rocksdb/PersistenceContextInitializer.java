package org.infinispan.persistence.rocksdb;

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
      includeClasses = {
            RocksDBStore.ExpiryBucket.class,
            RocksDBStore.MetadataImpl.class
      },
      schemaFileName = "persistence.rocksdb.proto",
      schemaFilePath = "proto/generated",
      schemaPackageName = "org.infinispan.persistence.rocksdb",
      service = false
)
interface PersistenceContextInitializer extends SerializationContextInitializer {
}
