package org.infinispan.client.hotrod.impl.transport.netty;

import java.util.Collections;
import java.util.List;

import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.DecoderException;
import io.netty.util.Signal;

/**
 * Copy-paste of {@link io.netty.handler.codec.ReplayingDecoder} which is hinted to not attempt decoding unless enough
 * bytes are read.
 *
 * The decoder does not expect pass different message up the pipeline, this is a terminal read operation.
 */
public abstract class HintedReplayingDecoder<S> extends ByteToMessageDecoder {
   static final Signal REPLAY = Signal.valueOf(HintedReplayingDecoder.class.getName() + ".REPLAY");
   // We don't expect decode() to use the out param
   private static final List<Object> NO_WRITE_LIST = Collections.emptyList();
   private static final Log log = LogFactory.getLog(HintedReplayingDecoder.class);

   private final HintingByteBuf replayable = new HintingByteBuf(this);
   private S state;
   private int checkpoint = -1;
   private int requiredReadableBytes = 0;

   /**
    * Creates a new instance with no initial state (i.e: {@code null}).
    */
   protected HintedReplayingDecoder() {
      this(null);
   }

   /**
    * Creates a new instance with the specified initial state.
    */
   protected HintedReplayingDecoder(S initialState) {
      state = initialState;
   }

   /**
    * Stores the internal cumulative buffer's reader position.
    */
   protected void checkpoint() {
      checkpoint = internalBuffer().readerIndex();
   }

   /**
    * Stores the internal cumulative buffer's reader position and updates
    * the current decoder state.
    */
   protected void checkpoint(S state) {
      checkpoint();
      state(state);
   }

   /**
    * Returns the current state of this decoder.
    * @return the current state of this decoder
    */
   protected S state() {
      return state;
   }

   /**
    * Sets the current state of this decoder.
    * @return the old state of this decoder
    */
   protected S state(S newState) {
      S oldState = state;
      state = newState;
      return oldState;
   }

   @Override
   public void channelInactive(ChannelHandlerContext ctx) throws Exception {
      replayable.terminate();
      super.channelInactive(ctx);
   }

   @Override
   protected void callDecode(ChannelHandlerContext ctx, ByteBuf in, List<Object> xxx) {
      if (in.readableBytes() < requiredReadableBytes) {
         // noop, wait for further reads
         return;
      }
      replayable.setCumulation(in);
      try {
         while (in.isReadable() && !ctx.isRemoved() && ctx.channel().isActive()) {
            checkpoint = in.readerIndex();

            try {
               decode(ctx, replayable, NO_WRITE_LIST);
               requiredReadableBytes = 0;
               // TODO: unset cumulation
            } catch (Signal replay) {
               replay.expect(REPLAY);

               // Check if this handler was removed before continuing the loop.
               // If it was removed, it is not safe to continue to operate on the buffer.
               //
               // See https://github.com/netty/netty/issues/1664
               if (ctx.isRemoved()) {
                  break;
               }

               // Return to the checkpoint (or oldPosition) and retry.
               int checkpoint = this.checkpoint;
               if (checkpoint >= 0) {
                  in.readerIndex(checkpoint);
               } else {
                  // Called by cleanup() - no need to maintain the readerIndex
                  // anymore because the buffer has been released already.
               }
               break;
            } catch (Throwable t) {
               requiredReadableBytes = 0;
               throw t;
            }
         }
      } catch (DecoderException e) {
         throw e;
      } catch (Throwable cause) {
         throw new DecoderException(cause);
      }
   }

   void requireWriterIndex(int index) {
      // TODO: setCumulator to composite if the number of bytes is too high
      requiredReadableBytes = index - checkpoint;
   }
}
