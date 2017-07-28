package org.infinispan.marshaller.kryo;

import org.infinispan.commons.dataconversion.MarshallerEncoder;

class KryoEncoder extends MarshallerEncoder {

   public KryoEncoder() {
      super(new KryoMarshaller());
   }

   @Override
   public short id() {
      return 1001;
   }
}
