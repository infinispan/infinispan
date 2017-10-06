package org.infinispan.marshaller.protostuff;

import org.infinispan.commons.dataconversion.MarshallerEncoder;

class ProtostuffEncoder extends MarshallerEncoder {

   public ProtostuffEncoder() {
      super(new ProtostuffMarshaller());
   }

   @Override
   public short id() {
      return 1002;
   }
}
