package org.infinispan.jcache.embedded;

import org.infinispan.jcache.annotation.DefaultCacheKey;
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
      dependsOn = org.infinispan.marshall.persistence.impl.PersistenceContextInitializer.class,
      includeClasses = DefaultCacheKey.class,
      schemaFileName = "persistence.jcache.proto",
      schemaFilePath = "proto/generated",
      schemaPackageName = "org.infinispan.persistence.jcache")
public interface PersistenceContextInitializer extends SerializationContextInitializer {
}
