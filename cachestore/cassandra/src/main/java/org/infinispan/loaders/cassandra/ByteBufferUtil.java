package org.infinispan.loaders.cassandra;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

public class ByteBufferUtil {

   public static final ByteBuffer EMPTY_BYTE_BUFFER = ByteBuffer.wrap(new byte[0]);
   public static final Charset UTF_8 = Charset.forName("UTF-8");

   public static ByteBuffer bytes(String s) {
      return ByteBuffer.wrap(s.getBytes(UTF_8));
   }

   public static ByteBuffer bytes(long l) {
      return ByteBuffer.allocate(8).putLong(0, l);
   }

}
