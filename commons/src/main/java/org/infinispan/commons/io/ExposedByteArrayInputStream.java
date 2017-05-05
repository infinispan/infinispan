package org.infinispan.commons.io;

import java.io.ByteArrayInputStream;

public class ExposedByteArrayInputStream extends ByteArrayInputStream {
   public ExposedByteArrayInputStream(byte[] buf) {
      super(buf);
   }

   public ExposedByteArrayInputStream(byte[] buf, int offset, int length) {
      super(buf, offset, length);
   }

   public byte[] getBuf() {
      return buf;
   }

   public int getOffset() {
      return pos;
   }

   public int getLength() {
      return count - pos;
   }
}
