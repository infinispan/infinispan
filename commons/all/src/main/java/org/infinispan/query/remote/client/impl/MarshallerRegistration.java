package org.infinispan.query.remote.client.impl;

import org.infinispan.protostream.SerializationContext;

/**
 * Registers protobuf schemas and marshallers for the objects used by remote query, remote continuous query and Ickle
 * based filters.
 *
 * @author anistor@redhat.com
 * @since 6.0
 */
public final class MarshallerRegistration {

   private MarshallerRegistration() {
   }

   /**
    * Registers proto files and marshallers.
    *
    * @param ctx the serialization context
    */
   public static void init(SerializationContext ctx) {
      GlobalContextInitializer.INSTANCE.register(ctx);
   }
}
