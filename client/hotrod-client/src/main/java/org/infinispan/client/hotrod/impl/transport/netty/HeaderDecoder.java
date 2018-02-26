package org.infinispan.client.hotrod.impl.transport.netty;

import java.net.SocketTimeoutException;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.impl.operations.HotRodOperation;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HeaderParams;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.Signal;

public class HeaderDecoder<T> extends HintedReplayingDecoder<HeaderDecoder.State> implements Runnable {
   public static final String NAME = "header-decoder";
   private final Codec codec;
   private final HeaderParams headerParams;
   private final ChannelFactory channelFactory;
   private final HotRodOperation<T> operation;

   private short status;
   private volatile ScheduledFuture<?> timeoutFuture;

   public HeaderDecoder(Codec codec, HeaderParams headerParams, ChannelFactory channelFactory, HotRodOperation<T> operation) {
      super(State.READ_HEADER);
      this.codec = codec;
      this.headerParams = headerParams;
      this.channelFactory = channelFactory;
      this.operation = operation;
   }

   @Override
   public boolean isSharable() {
      return false;
   }

   @Override
   public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
      // we don't have to synchronize this because the handler is added before we send the request,
      // therefore before we can start receiving the response that would cancel this future
      timeoutFuture = ctx.executor().schedule(this, channelFactory.socketTimeout(), TimeUnit.MILLISECONDS);
   }

   @Override
   protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
      switch (state()) {
         case READ_HEADER:
            try {
               status = codec.readHeader(in, headerParams, channelFactory, ctx.channel().remoteAddress());
            } catch (Signal signal) {
               throw signal;
            } catch (Throwable t) {
               // When we encounter an exception reading the header, we need to remove this decoder
               // as we'll probably retry. The exceptionCaught handler is responsible for removing itself
               // if it does not close the channel completely.
               ctx.pipeline().remove(this);
               throw t;
            }
            checkpoint(State.READ_PAYLOAD);
         case READ_PAYLOAD:
            T result = operation.decodePayload(in, status);
            try {
               ctx.pipeline().remove(this);
               ctx.pipeline().remove(operation);
               operation.releaseChannel(ctx.channel());
               timeoutFuture.cancel(false);
            } catch (Throwable t) {
               try {
                  ctx.channel().close();
               } catch (Throwable t2) {
                  t.addSuppressed(t2);
               }
               operation.completeExceptionally(t);
               return;
            }
            operation.complete(result);
            break;
      }
   }

   /**
    * {@inheritDoc}
    *
    * Checkpoint is exposed for implementations of {@link HotRodOperation}
    */
   @Override
   public void checkpoint() {
      super.checkpoint();
   }

   @Override
   public void run() {
      operation.completeExceptionally(new SocketTimeoutException(
            operation + " timed out after " + channelFactory.socketTimeout() + " ms"));
   }

   enum State {
      READ_HEADER,
      READ_PAYLOAD
   }
}
