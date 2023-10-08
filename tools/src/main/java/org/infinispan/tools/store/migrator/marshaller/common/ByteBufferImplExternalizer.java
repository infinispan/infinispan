package org.infinispan.tools.store.migrator.marshaller.common;

import java.io.IOException;
import java.io.ObjectInput;

import org.infinispan.commons.io.ByteBufferImpl;
import org.infinispan.commons.io.UnsignedNumeric;

public class ByteBufferImplExternalizer extends AbstractMigratorExternalizer<ByteBufferImpl> {

   public ByteBufferImplExternalizer() {
      super(ByteBufferImpl.class, Ids.BYTE_BUFFER);
   }

   @Override
   public ByteBufferImpl readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      int length = UnsignedNumeric.readUnsignedInt(input);
      byte[] data = new byte[length];
      input.readFully(data, 0, length);
      return ByteBufferImpl.create(data, 0, length);
   }
}
