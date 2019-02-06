package org.infinispan.tools.store.migrator.marshaller.infinispan9;

import java.io.IOException;

import org.infinispan.marshall.core.AbstractBytesObjectInput;

class BytesObjectInput extends AbstractBytesObjectInput {

   private final Infinispan9Marshaller marshaller;

   BytesObjectInput(byte[] bytes, int offset, Infinispan9Marshaller marshaller) {
      super(bytes, offset);
      this.marshaller = marshaller;
   }

   @Override
   public Object readObject() throws ClassNotFoundException, IOException {
      return marshaller.readNullableObject(this);
   }
}
