package org.infinispan.server.resp;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.util.concurrent.CompletionStages;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.AttributeKey;

public abstract class RespRequestHandler {
   protected final CompletionStage<RespRequestHandler> myStage = CompletableFuture.completedFuture(this);

   public static final AttributeKey<ByteBufPool> BYTE_BUF_POOL_ATTRIBUTE_KEY = AttributeKey.newInstance("buffer-pool");

   ByteBufPool allocatorToUse;

   protected void initializeIfNecessary(ChannelHandlerContext ctx) {
      if (allocatorToUse == null) {
         if (!ctx.channel().hasAttr(BYTE_BUF_POOL_ATTRIBUTE_KEY)) {
            throw new IllegalStateException("BufferPool was not initialized in the context " + ctx);
         }
         allocatorToUse = ctx.channel().attr(BYTE_BUF_POOL_ATTRIBUTE_KEY).get();
      }
   }

   public final CompletionStage<RespRequestHandler> handleRequest(ChannelHandlerContext ctx, RespCommand type, List<byte[]> arguments) {
      initializeIfNecessary(ctx);
      if (type == null) {
         stringToByteBuf("-ERR unknown command\r\n", allocatorToUse);
         return myStage;
      }
      return actualHandleRequest(ctx, type, arguments);
   }

   /**
    * Handles the RESP request returning a stage that when complete notifies the command has completed as well as
    * providing the request handler for subsequent commands.
    * <p>
    * Implementations should never use the ByteBufAllocator in the context.
    * Instead, they should use {@link #allocatorToUse} to retrieve a ByteBuffer.
    * This ByteBuffer should only have bytes written to it adding up to the size requested.
    * The ByteBuffer itself should never be written to the context or channel and flush should also never be invoked.
    * Failure to do so may cause mis-ordering or responses as requests support pipelining and a ByteBuf may not be
    * send down stream until later in the pipeline.
    *
    * @param ctx Netty context pipeline for this request
    * @param type The command type
    * @param arguments The remaining arguments to the command
    * @return stage that when complete returns the new handler to instate. This stage <b>must</b> be completed on the event loop
    */
   protected CompletionStage<RespRequestHandler> actualHandleRequest(ChannelHandlerContext ctx, RespCommand type, List<byte[]> arguments) {
      if (type == RespCommand.QUIT) {
         ctx.close();
         return myStage;
      }
      stringToByteBuf("-ERR unknown command\r\n", allocatorToUse);
      return myStage;
   }

   public void handleChannelDisconnect(ChannelHandlerContext ctx) {
      // Do nothing by default
   }

   protected <E> CompletionStage<RespRequestHandler> stageToReturn(CompletionStage<E> stage, ChannelHandlerContext ctx,
         BiConsumer<? super E, ByteBufPool> biConsumer) {
      return stageToReturn(stage, ctx, Objects.requireNonNull(biConsumer), null);
   }

   protected <E> CompletionStage<RespRequestHandler> stageToReturn(CompletionStage<E> stage, ChannelHandlerContext ctx,
         Function<E, RespRequestHandler> handlerWhenComplete) {
      return stageToReturn(stage, ctx, null, Objects.requireNonNull(handlerWhenComplete));
   }

   /**
    * Handles ensuring that a stage TriConsumer is only invoked on the event loop. The TriConsumer can then do things
    * such as writing to the underlying channel or update variables in a thread safe manner without additional
    * synchronization or volatile variables.
    * <p>
    * If the TriConsumer provided is null the provided stage <b>must</b> be completed only on the event loop of the
    * provided ctx.
    * This can be done via the Async methods on {@link CompletionStage} such as {@link CompletionStage#thenApplyAsync(Function, Executor)};
    * <p>
    * If the returned stage was completed exceptionally, any exception is ignored, assumed to be handled in the
    * <b>biConsumer</b> or further up the stage.
    * The <b>handlerWhenComplete</b> is not invoked if the stage was completed exceptionally as well.
    * If the <b>triConsumer</b> or <b>handlerWhenComplete</b> throw an exception when invoked the returned stage will
    * complete with that exception.
    * <p>
    * This method can only be invoked on the event loop for the provided ctx.
    *
    * @param <E> The stage return value
    * @param stage The stage that will complete. Note that if biConsumer is null this stage may only be completed on the event loop
    * @param ctx The context used for this request, normally the event loop is the thing used
    * @param biConsumer The consumer to be ran on
    * @param handlerWhenComplete A function to map the result to which handler will be used for future requests. Only ever invoked on the event loop. May be null, if so the current handler is returned
    * @return A stage that is only ever completed on the event loop that provides the new handler to use
    */
   private <E> CompletionStage<RespRequestHandler> stageToReturn(CompletionStage<E> stage, ChannelHandlerContext ctx,
         BiConsumer<? super E, ByteBufPool> biConsumer, Function<E, RespRequestHandler> handlerWhenComplete) {
      assert ctx.channel().eventLoop().inEventLoop();
      // Only one or the other can be null
      assert (biConsumer != null && handlerWhenComplete == null) || (biConsumer == null && handlerWhenComplete != null) :
            "triConsumer was: " + biConsumer + " and handlerWhenComplete was: " + handlerWhenComplete;
      if (CompletionStages.isCompletedSuccessfully(stage)) {
         E result = CompletionStages.join(stage);
         try {
            if (handlerWhenComplete != null) {
               return CompletableFuture.completedFuture(handlerWhenComplete.apply(result));
            }
            biConsumer.accept(result, allocatorToUse);
         } catch (Throwable t) {
            return CompletableFutures.completedExceptionFuture(t);
         }
         return myStage;
      }
      if (biConsumer != null) {
         // Note that this method is only ever invoked in the event loop, so this whenCompleteAsync can never complete
         // until this request completes, meaning the thenApply will always be invoked in the event loop as well
         return CompletionStages.handleAndComposeAsync(stage, (e, t) -> {
            if (t != null) {
               Resp3Handler.handleThrowable(allocatorToUse, t);
            } else {
               try {
                  biConsumer.accept(e, allocatorToUse);
               } catch (Throwable innerT) {
                  return CompletableFutures.completedExceptionFuture(innerT);
               }
            }
            return myStage;
         }, ctx.channel().eventLoop());
      }
      return stage.handle((value, t) -> {
         if (t != null) {
            // If exception, never use handler
            return this;
         }
         return handlerWhenComplete.apply(value);
      });
   }

   protected static ByteBuf stringToByteBufWithExtra(CharSequence string, ByteBufPool alloc, int extraBytes) {
      boolean release = true;
      int stringBytes = ByteBufUtil.utf8Bytes(string);
      int allocatedSize = stringBytes + extraBytes;
      ByteBuf buffer = alloc.apply(allocatedSize);

      try {
         int beforeWriteIndex = buffer.writerIndex();
         ByteBufUtil.reserveAndWriteUtf8(buffer, string, allocatedSize);
         assert buffer.capacity() - buffer.writerIndex() > extraBytes;
         assert buffer.writerIndex() - beforeWriteIndex == stringBytes;
         release = false;
      } finally {
         if (release) {
            buffer.release();
         }
      }

      return buffer;
   }

   protected static ByteBuf stringToByteBuf(CharSequence string, ByteBufPool alloc) {
      return stringToByteBufWithExtra(string, alloc, 0);
   }

   protected static ByteBuf bytesToResult(byte[] result, ByteBufPool alloc) {
      int length = result.length;
      int stringLength = stringSize(length);

      // Need 5 extra for $ and 2 sets of /r/n
      int exactSize = stringLength + length + 5;
      ByteBuf buffer = alloc.acquire(exactSize);
      buffer.writeByte('$');
      // This method is anywhere from 10-100% faster than ByteBufUtil.writeAscii and avoids allocations
      setIntChars(length, stringLength, buffer);
      buffer.writeByte('\r').writeByte('\n');
      buffer.writeBytes(result);
      buffer.writeByte('\r').writeByte('\n');

      return buffer;
   }

   protected static int stringSize(int x) {
      int d = 1;
      if (x >= 0) {
         d = 0;
         x = -x;
      }
      int p = -10;
      for (int i = 1; i < 10; i++) {
         if (x > p)
            return i + d;
         p = 10 * p;
      }
      return 10 + d;
   }

   // This code is a modified version of Integer.toString to write the underlying bytes directly to the ByteBuffer
   // instead of creating a String around a byte[]
   protected static int setIntChars(int i, int index, ByteBuf buf) {
      int writeIndex = buf.writerIndex();
      int q, r;
      int charPos = index;

      boolean negative = i < 0;
      if (!negative) {
         i = -i;
      }

      // Generate two digits per iteration
      while (i <= -100) {
         q = i / 100;
         r = (q * 100) - i;
         i = q;
         buf.setByte(writeIndex + --charPos, DigitOnes[r]);
         buf.setByte(writeIndex + --charPos, DigitTens[r]);
      }

      // We know there are at most two digits left at this point.
      q = i / 10;
      r = (q * 10) - i;
      buf.setByte(writeIndex + --charPos, (byte) ('0' + r));

      // Whatever left is the remaining digit.
      if (q < 0) {
         buf.setByte(writeIndex + --charPos, (byte) ('0' - q));
      }

      if (negative) {
         buf.setByte(writeIndex + --charPos, (byte) '-');
      }
      buf.writerIndex(writeIndex + index);
      return charPos;
   }

   static final byte[] DigitTens = {
         '0', '0', '0', '0', '0', '0', '0', '0', '0', '0',
         '1', '1', '1', '1', '1', '1', '1', '1', '1', '1',
         '2', '2', '2', '2', '2', '2', '2', '2', '2', '2',
         '3', '3', '3', '3', '3', '3', '3', '3', '3', '3',
         '4', '4', '4', '4', '4', '4', '4', '4', '4', '4',
         '5', '5', '5', '5', '5', '5', '5', '5', '5', '5',
         '6', '6', '6', '6', '6', '6', '6', '6', '6', '6',
         '7', '7', '7', '7', '7', '7', '7', '7', '7', '7',
         '8', '8', '8', '8', '8', '8', '8', '8', '8', '8',
         '9', '9', '9', '9', '9', '9', '9', '9', '9', '9',
   };

   static final byte[] DigitOnes = {
         '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
         '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
         '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
         '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
         '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
         '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
         '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
         '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
         '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
         '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
   };
}
