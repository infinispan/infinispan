package org.infinispan.server.core.transport;

import org.infinispan.commons.netty.VarintEncodeDecode;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.CorruptedFrameException;

/**
 * Reads and writes unsigned variable length long values. Even though it's deprecated, do not
 * remove from source code for the moment because it's a good scala example and could be used
 * as reference.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
public class VLong {

   public static void write(ByteBuf out, long i) {
      VarintEncodeDecode.writeVLong(out, i);
   }

   public static long read(ByteBuf in) {
      int b = in.readerIndex();
      long v = VarintEncodeDecode.readVLong(in);
      if (b == in.readerIndex())
         throw new CorruptedFrameException("Not enough bytes for long");
      return v;
   }
}
