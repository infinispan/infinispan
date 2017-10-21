package org.infinispan.marshaller.kryo;

import org.infinispan.commons.dataconversion.MarshallerEncoder;
import org.infinispan.commons.dataconversion.MediaType;

class KryoEncoder extends MarshallerEncoder {

   public KryoEncoder() {
      super(new KryoMarshaller());
   }

   @Override
   public MediaType getStorageFormat() {
      return MediaType.APPLICATION_KRYO;
   }

   @Override
   public short id() {
      return 1001;
   }
}
