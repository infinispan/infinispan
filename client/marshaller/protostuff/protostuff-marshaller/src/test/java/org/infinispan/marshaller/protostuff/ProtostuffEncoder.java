package org.infinispan.marshaller.protostuff;

import org.infinispan.commons.dataconversion.MarshallerEncoder;
import org.infinispan.commons.dataconversion.MediaType;

class ProtostuffEncoder extends MarshallerEncoder {

   public ProtostuffEncoder() {
      super(new ProtostuffMarshaller());
   }

   @Override
   public MediaType getStorageFormat() {
      return MediaType.APPLICATION_PROTOSTUFF;
   }

   @Override
   public short id() {
      return 1002;
   }
}
