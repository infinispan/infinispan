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
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandlerContext;

public abstract class RespRequestHandler {
   protected final CompletionStage<RespRequestHandler> myStage = CompletableFuture.completedStage(this);

   /**
    * Handles the RESP request returning a stage that when complete notifies the command has completed as well as
    * providing the request handler for subsequent commands.
    *
    * @param ctx Netty context pipeline for this request
    * @param type The command type
    * @param arguments The remaining arguments to the command
    * @return stage that when complete returns the new handler to instate. This stage <b>must</b> be completed on the event loop
    */
   public CompletionStage<RespRequestHandler> handleRequest(ChannelHandlerContext ctx, String type, List<byte[]> arguments) {
      if ("QUIT".equals(type)) {
         ctx.close();
         return myStage;
      }
      ctx.writeAndFlush(stringToByteBuf("-ERR unknown command\r\n", ctx.alloc()));
      return myStage;
   }

   public void handleChannelDisconnect(ChannelHandlerContext ctx) {
      // Do nothing by default
   }

   protected <E> CompletionStage<RespRequestHandler> stageToReturn(CompletionStage<E> stage, ChannelHandlerContext ctx,
         BiConsumer<? super E, Throwable> biConsumer) {
      return stageToReturn(stage, ctx, Objects.requireNonNull(biConsumer), null);
   }

   protected <E> CompletionStage<RespRequestHandler> stageToReturn(CompletionStage<E> stage, ChannelHandlerContext ctx,
         Function<E, RespRequestHandler> handlerWhenComplete) {
      return stageToReturn(stage, ctx, null, Objects.requireNonNull(handlerWhenComplete));
   }

   /**
    * Handles ensuring that a stage BiConsumer is only invoked on the event loop. The BiConsumer can then do things
    * such as writing to the underlying channel or update variables in a thread safe manner without additional
    * synchronization or volatile variables.
    * <p>
    * If the BiConsumer provided is null the provided stage <b>must</b> be completed only on the event loop of the
    * provided ctx.
    * This can be done via the Async methods on {@link CompletionStage} such as {@link CompletionStage#thenApplyAsync(Function, Executor)};
    * <p>
    * If the returned stage was completed exceptionally, any exception is ignored, assumed to be handled in the
    * <b>biConsumer</b> or further up the stage.
    * The <b>handlerWhenComplete</b> is not invoked if the stage was completed exceptionally as well.
    * If the <b>biConsumer</b> or <b>handlerWhenComplete</b> throw an exception when invoked the returned stage will
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
   private <E> CompletionStage<RespRequestHandler> stageToReturn(CompletionStage<E> stage, ChannelHandlerContext ctx, BiConsumer<? super E, Throwable> biConsumer,
         Function<E, RespRequestHandler> handlerWhenComplete) {
      assert ctx.channel().eventLoop().inEventLoop();
      // Only one or the other can be null
      assert (biConsumer != null && handlerWhenComplete == null) || (biConsumer == null && handlerWhenComplete != null) :
            "biConsumer was: " + biConsumer + " and handlerWhenComplete was: " + handlerWhenComplete;
      if (CompletionStages.isCompletedSuccessfully(stage)) {
         E result = CompletionStages.join(stage);
         try {
            if (handlerWhenComplete != null) {
               return CompletableFuture.completedFuture(handlerWhenComplete.apply(result));
            }
            biConsumer.accept(result, null);
         } catch (Throwable t) {
            return CompletableFutures.completedExceptionFuture(t);
         }
         return myStage;
      }
      if (biConsumer != null) {
         // Note that this method is only ever invoked in the event loop, so this whenCompleteAsync can never complete
         // until this request completes, meaning the thenApply will always be invoked in the event loop as well
         return CompletionStages.handleAndComposeAsync(stage, (e, t) -> {
            try {
               biConsumer.accept(e, t);
               return myStage;
            } catch (Throwable innerT) {
               if (t != null) {
                  innerT.addSuppressed(t);
               }
               return CompletableFutures.completedExceptionFuture(innerT);
            }
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

   static ByteBuf stringToByteBufWithExtra(CharSequence string, ByteBufAllocator allocator, int extraBytes) {
      boolean release = true;
      ByteBuf buffer = allocator.buffer(ByteBufUtil.utf8Bytes(string) + extraBytes);

      try {
         ByteBufUtil.writeUtf8(buffer, string);
         release = false;
      } finally {
         if (release) {
            buffer.release();
         }
      }

      return buffer;
   }

   static ByteBuf stringToByteBuf(CharSequence string, ByteBufAllocator allocator) {
      return stringToByteBufWithExtra(string, allocator, 0);
   }
}
