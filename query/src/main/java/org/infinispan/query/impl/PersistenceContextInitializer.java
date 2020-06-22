package org.infinispan.query.impl;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.marshall.persistence.impl.PersistenceMarshallerImpl;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Interface used to initialise the {@link PersistenceMarshallerImpl}'s {@link org.infinispan.protostream.SerializationContext}
 * using the specified Pojos, Marshaller implementations and provided .proto schemas.
 *
 * @author Ryan Emerson
 * @since 10.0
 */
@AutoProtoSchemaBuilder(
      includeClasses = PersistenceContextInitializer.KnownClassKey.class,
      schemaFileName = "persistence.query.proto",
      schemaFilePath = "proto/generated",
      schemaPackageName = "org.infinispan.persistence.query",
      service = false
)
interface PersistenceContextInitializer extends SerializationContextInitializer {

   //TODO [anistor] remove this !
   @ProtoTypeId(ProtoStreamTypeIds.QUERY_LOWER_BOUND)
   public static final class KnownClassKey {

      @ProtoField(number = 1)
      public final String cacheName;

      @ProtoField(number = 2)
      public final String className;

      @ProtoFactory
      public KnownClassKey(String cacheName, String className) {
         this.cacheName = cacheName;
         this.className = className;
      }
   }
}
