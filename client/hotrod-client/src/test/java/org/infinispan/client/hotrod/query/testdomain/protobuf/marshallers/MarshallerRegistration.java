package org.infinispan.client.hotrod.query.testdomain.protobuf.marshallers;

import java.io.IOException;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.marshall.MarshallerUtil;
import org.infinispan.protostream.SerializationContext;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class MarshallerRegistration {

   private MarshallerRegistration() {
   }

   public static void registerMarshallers(RemoteCacheManager remoteCacheManager) throws IOException {
      registerMarshallers(MarshallerUtil.getSerializationContext(remoteCacheManager));
   }

   /**
    * Registers proto files and marshallers.
    *
    * @param ctx the serialization context
    * @throws org.infinispan.protostream.DescriptorParserException if a proto definition file fails to parse correctly
    * @throws IOException if proto file registration fails
    */
   public static void registerMarshallers(SerializationContext ctx) throws IOException {
      TestDomainSCI.INSTANCE.registerSchema(ctx);
      TestDomainSCI.INSTANCE.registerMarshallers(ctx);
   }
}
