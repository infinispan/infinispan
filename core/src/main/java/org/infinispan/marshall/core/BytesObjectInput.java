package org.infinispan.marshall.core;

import java.io.IOException;
import java.io.ObjectInput;

/**
 * Array backed {@link ObjectInput} implementation.
 *
 * {@link #skip(long)} and {@link #skipBytes(int)} have been enhanced so that
 * if a negative number is passed in, they skip backwards effectively
 * providing rewind capabilities.
 */
final class BytesObjectInput extends AbstractBytesObjectInput {

   final GlobalMarshaller marshaller;

   private BytesObjectInput(byte[] bytes, int offset, GlobalMarshaller marshaller) {
      super(bytes, offset);
      this.marshaller = marshaller;
   }

   static BytesObjectInput from(byte[] bytes, GlobalMarshaller marshaller) {
      return from(bytes, 0, marshaller);
   }

   static BytesObjectInput from(byte[] bytes, int offset, GlobalMarshaller marshaller) {
      return new BytesObjectInput(bytes, offset, marshaller);
   }

   @Override
   public Object readObject() throws ClassNotFoundException, IOException {
      return marshaller.readNullableObject(this);
   }
}
