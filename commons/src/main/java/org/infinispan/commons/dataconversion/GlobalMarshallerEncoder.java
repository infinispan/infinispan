package org.infinispan.commons.dataconversion;

import org.infinispan.commons.marshall.Marshaller;

/**
 * Encoder that uses the GlobalMarshaller to encode/decode data.
 *
 * @since 9.2
 */
public class GlobalMarshallerEncoder extends MarshallerEncoder {

   public GlobalMarshallerEncoder(Marshaller globalMarshaller) {
      super(globalMarshaller);
   }

   @Override
   public MediaType getStorageFormat() {
      return MediaType.APPLICATION_INFINISPAN_MARSHALLED;
   }

   @Override
   public short id() {
      return EncoderIds.GLOBAL_MARSHALLER;
   }
}
