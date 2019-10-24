package org.infinispan.marshall.persistence.impl;

import org.infinispan.container.entries.RemoteMetadata;
import org.infinispan.container.versioning.NumericVersion;
import org.infinispan.container.versioning.SimpleClusteredVersion;
import org.infinispan.functional.impl.MetaParamsInternalMetadata;
import org.infinispan.marshall.protostream.impl.MarshallableUserObject;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;
import org.infinispan.remoting.transport.jgroups.JGroupsAddress;
import org.infinispan.util.ByteString;
import org.infinispan.util.logging.events.EventLogCategory;
import org.infinispan.util.logging.events.EventLogLevel;

/**
 * Interface used to initialise the {@link PersistenceMarshallerImpl}'s {@link org.infinispan.protostream.SerializationContext}
 * using the specified Pojos, Marshaller implementations and provided .proto schemas.
 *
 * @author Ryan Emerson
 * @since 10.0
 */
@AutoProtoSchemaBuilder(
      dependsOn = org.infinispan.commons.marshall.PersistenceContextInitializer.class,
      includeClasses = {
            ByteString.class,
            EmbeddedMetadata.class,
            EmbeddedMetadata.EmbeddedExpirableMetadata.class,
            EmbeddedMetadata.EmbeddedLifespanExpirableMetadata.class,
            EmbeddedMetadata.EmbeddedMaxIdleExpirableMetadata.class,
            EventLogCategory.class,
            EventLogLevel.class,
            JGroupsAddress.class,
            MarshalledValueImpl.class,
            MetaParamsInternalMetadata.class,
            NumericVersion.class,
            RemoteMetadata.class,
            SimpleClusteredVersion.class,
            MarshallableUserObject.class
      },
      schemaFileName = "persistence.core.proto",
      schemaFilePath = "proto/generated",
      schemaPackageName = PersistenceContextInitializer.PACKAGE_NAME)
public interface PersistenceContextInitializer extends SerializationContextInitializer {
   String PACKAGE_NAME = "org.infinispan.persistence.core";

   static String getFqTypeName(Class clazz) {
      return PACKAGE_NAME + "." + clazz.getSimpleName();
   }
}
