package org.infinispan.lucene.impl;

import org.infinispan.lucene.ChunkCacheKey;
import org.infinispan.lucene.FileCacheKey;
import org.infinispan.lucene.FileListCacheKey;
import org.infinispan.lucene.FileMetadata;
import org.infinispan.lucene.FileReadLockKey;
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
            ChunkCacheKey.class,
            FileCacheKey.class,
            FileListCacheKey.class,
            FileMetadata.class,
            FileReadLockKey.class,
            FileListCacheValue.class
      },
      schemaFileName = "persistence.lucene.proto",
      schemaFilePath = "proto/generated",
      schemaPackageName = "org.infinispan.persistence.lucene")
public interface PersistenceContextInitializer extends SerializationContextInitializer {
}
