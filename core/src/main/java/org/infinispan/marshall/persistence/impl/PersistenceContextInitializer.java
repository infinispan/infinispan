package org.infinispan.marshall.persistence.impl;

import org.infinispan.container.entries.RemoteMetadata;
import org.infinispan.container.versioning.NumericVersion;
import org.infinispan.container.versioning.SimpleClusteredVersion;
import org.infinispan.container.versioning.irac.IracEntryVersion;
import org.infinispan.container.versioning.irac.TopologyIracVersion;
import org.infinispan.functional.impl.MetaParamsInternalMetadata;
import org.infinispan.marshall.protostream.impl.MarshallableUserObject;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.impl.IracMetadata;
import org.infinispan.metadata.impl.PrivateMetadata;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.ProtoSchema;
import org.infinispan.protostream.annotations.ProtoSyntax;
import org.infinispan.remoting.transport.jgroups.JGroupsAddress;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.impl.CacheRoleImpl;
import org.infinispan.marshall.protostream.impl.adapters.SubjectAdapter;
import org.infinispan.security.mappers.ClusterRoleMapper;
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
@ProtoSchema(
      dependsOn = {
            org.infinispan.commons.marshall.PersistenceContextInitializer.class,
            org.infinispan.counter.api._private.PersistenceContextInitializer.class
      },
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
            MarshallableUserObject.class,
            PrivateMetadata.class,
            IracEntryVersion.class,
            IracEntryVersion.MapEntry.class,
            TopologyIracVersion.class,
            IracMetadata.class,
            ClusterRoleMapper.RoleSet.class,
            AuthorizationPermission.class,
            CacheRoleImpl.class,
            SubjectAdapter.class
      },
      schemaFileName = "persistence.core.proto",
      schemaFilePath = "proto/generated",
      schemaPackageName = PersistenceContextInitializer.PACKAGE_NAME,
      service = false,
      syntax = ProtoSyntax.PROTO2
)
public interface PersistenceContextInitializer extends SerializationContextInitializer {

   SerializationContextInitializer INSTANCE = new PersistenceContextInitializerImpl();

   String PACKAGE_NAME = "org.infinispan.persistence.core";

   static String getFqTypeName(Class<?> clazz) {
      return PACKAGE_NAME + "." + clazz.getSimpleName();
   }
}
