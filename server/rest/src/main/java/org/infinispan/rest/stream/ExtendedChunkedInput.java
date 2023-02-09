package org.infinispan.rest.stream;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.stream.ChunkedInput;
import io.netty.handler.stream.ChunkedWriteHandler;

public interface ExtendedChunkedInput<T> extends ChunkedInput<T> {

   /**
    * Expose a method to set the {@link ChannelHandlerContext}. The {@link io.netty.handler.stream.ChunkedWriteHandler}
    * is necessary when the {@link ChunkedInput} can return <code>null</code> values because the elements in the
    * stream are momentarily unavailable. This makes it easier for the {@link ChunkedInput} to call
    * {@link ChunkedWriteHandler#resumeTransfer()}.
    *
    * @param ctx: The current context.
    */
   void setContext(ChannelHandlerContext ctx);
}
