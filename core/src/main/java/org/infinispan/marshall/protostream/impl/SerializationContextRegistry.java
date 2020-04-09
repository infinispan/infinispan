package org.infinispan.marshall.protostream.impl;

import org.infinispan.protostream.BaseMarshaller;
import org.infinispan.protostream.FileDescriptorSource;
import org.infinispan.protostream.ImmutableSerializationContext;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.SerializationContextInitializer;

/**
 * Manages {@link SerializationContext} across modules for use by various components. The user context should only
 * ever be updated using the {@link org.infinispan.configuration.global.GlobalConfiguration}, therefore we do not expose
 * it via {@link MarshallerType}.
 *
 * @author Ryan Emerson
 * @since 10.0
 */
public interface SerializationContextRegistry {

   void addProtoFile(MarshallerType type, FileDescriptorSource fileDescriptorSource);

   void addMarshaller(MarshallerType type, BaseMarshaller marshaller);

   void addContextInitializer(MarshallerType type, SerializationContextInitializer sci);

   ImmutableSerializationContext getGlobalCtx();

   ImmutableSerializationContext getPersistenceCtx();

   ImmutableSerializationContext getUserCtx();

   enum MarshallerType {
      GLOBAL,
      PERSISTENCE
   }
}
