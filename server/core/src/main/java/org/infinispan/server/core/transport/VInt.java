package org.infinispan.server.core.transport;

import org.infinispan.commons.netty.VarintEncodeDecode;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.CorruptedFrameException;

/**
 * Reads and writes unsigned variable length integer values. Even though it's deprecated, do not
 * remove from source code for the moment because it's a good scala example and could be used
 * as reference.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
public class VInt {

   public static void write(ByteBuf out, int i) {
      VarintEncodeDecode.writeVInt(out, i);
   }

   public static int read(ByteBuf in) {
      int before = in.readerIndex();
      int value = VarintEncodeDecode.readVInt(in);
      if (before == in.readerIndex())
         throw new CorruptedFrameException("Unable to read vint");
      return value;
   }
}
