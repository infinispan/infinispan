package org.infinispan.server.resp;

import java.util.function.IntFunction;

import io.netty.buffer.ByteBuf;

@FunctionalInterface
public interface ByteBufPool extends IntFunction<ByteBuf> {
   /**
    * This method will return a pooled ByteBuf.
    * This buffer may already have bytes written to it.
    * A caller should only ever write additional bytes to the buffer and not change it in any other way.
    * <p>
    * The returned ByteBuf should never be written as the decoder will handle this instead
    *
    * @param requiredSize The amount of bytes required to be writable into the ByteBuf
    * @return a ByteBuf to write those bytes to
    */
   default ByteBuf acquire(int requiredSize) {
      return apply(requiredSize);
   }
}
