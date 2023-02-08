package org.infinispan.server.resp;

import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.util.Util;
import org.infinispan.server.resp.logging.Log;
import org.infinispan.util.concurrent.CompletionStages;

import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.output.CommandOutput;
import io.lettuce.core.protocol.RedisStateMachine;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

public class RespLettuceHandler extends ByteToMessageDecoder {
   private final static Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass(), Log.class);

   private final RedisStateMachine stateMachine = new RedisStateMachine(ByteBufAllocator.DEFAULT);
   private RespRequestHandler requestHandler;
   private final OurCommandOutput<byte[], byte[]> currentOutput = new OurCommandOutput<>(ByteArrayCodec.INSTANCE);
   private boolean disabledRead = false;

   public RespLettuceHandler(RespRequestHandler initialHandler) {
      this.requestHandler = initialHandler;
   }

   @Override
   public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
      super.channelUnregistered(ctx);
      stateMachine.close();
      requestHandler.handleChannelDisconnect(ctx);
   }

   private static class OurCommandOutput<K, V> extends CommandOutput<K, V, List<Object>> {

      /**
       * Initialize a new instance that encodes and decodes keys and values using the supplied codec.
       *
       * @param codec Codec used to encode/decode keys and values, must not be {@code null}.
       */
      public OurCommandOutput(RedisCodec<K, V> codec) {
         super(codec, Collections.emptyList());
         stack = new ArrayDeque<>();
      }

      public void reset() {
         stack.clear();
         output.clear();
         depth = 0;
         type = null;
      }

      // Copied from PushOutput
      private String type;

      @Override
      public void set(ByteBuffer bytes) {

         if (type == null) {
            bytes.mark();
            // TODO: avoid DirectByteBuffer allocation
            type = StringCodec.ASCII.decodeKey(bytes);
            return;
         }

         if (!initialized) {
            output = new ArrayList<>();
         }

         output.add(bytes == null ? null : codec.decodeValue(bytes));
      }

      @Override
      public void setSingle(ByteBuffer bytes) {
         if (type == null) {
            set(bytes);
         }
         if (!initialized) {
            output = new ArrayList<>();
         }

         output.add(bytes == null ? null : StringCodec.UTF8.decodeValue(bytes));
      }

      public String type() {
         return type;
      }

      public List<Object> getContent() {

         List<Object> copy = new ArrayList<>();

         for (Object o : get()) {
            if (o instanceof ByteBuffer) {
               copy.add(((ByteBuffer) o).asReadOnlyBuffer());
            } else {
               copy.add(o);
            }
         }

         return Collections.unmodifiableList(copy);
      }

      // Copied from NestedMultiOutput

      private final Deque<List<Object>> stack;

      private int depth;

      private boolean initialized;

      @Override
      public void set(long integer) {

         if (!initialized) {
            output = new ArrayList<>();
         }

         output.add(integer);
      }

      @Override
      public void set(double number) {

         if (!initialized) {
            output = new ArrayList<>();
         }

         output.add(number);
      }

      @Override
      public void complete(int depth) {
         if (depth > 0 && depth < this.depth) {
            output = stack.pop();
            this.depth--;
         }
      }

      @Override
      public void multi(int count) {

         if (!initialized) {
            output = new ArrayList<>(count);
            initialized = true;
         }

         List<Object> a = new ArrayList<>(count);
         output.add(a);
         stack.push(output);
         output = a;
         this.depth++;
      }
   }

   @Override
   protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
      // Don't read any of the ByteBuf if we disabled reads
      if (disabledRead) {
         return;
      }
      if (stateMachine.decode(in, currentOutput)) {
         String type = currentOutput.type().toUpperCase();
         // Read a complete command, use a new one for next round
         List<byte[]> contentToUse = (List) currentOutput.getContent();
         currentOutput.reset();
         if (log.isTraceEnabled()) {
            log.tracef("Received command: %s with arguments %s", type, Util.toStr(contentToUse));
         }
         CompletionStage<RespRequestHandler> stage = requestHandler.handleRequest(ctx, type, contentToUse);
         if (CompletionStages.isCompletedSuccessfully(stage)) {
            requestHandler = CompletionStages.join(stage);
         } else {
            log.tracef("Disabling auto read for channel %s until previous command is complete", ctx.channel());
            // Disable reading any more from socket - until command is complete
            ctx.channel().config().setAutoRead(false);
            disabledRead = true;
            stage.whenComplete((handler, t) -> {
               assert ctx.channel().eventLoop().inEventLoop();
               log.tracef("Re-enabling auto read for channel %s as previous command is complete", ctx.channel());
               ctx.channel().config().setAutoRead(true);
               disabledRead = false;
               if (t != null) {
                  exceptionCaught(ctx, t);
               } else {
                  // Instate the new handler if there was no exception
                  requestHandler = handler;
               }

               // If there is any readable bytes left before we paused make sure to try to decode, just in case
               // if a pending message was read before we disabled auto read
               ByteBuf buf = internalBuffer();
               if (buf.isReadable()) {
                  log.tracef("Bytes available from previous read for channel %s, trying decode directly", ctx.channel());
                  // callDecode will call us until the ByteBuf is no longer consumed
                  callDecode(ctx, buf, List.of());
               }
            });
         }
      }
   }

   @Override
   public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      log.unexpectedException(cause);
      ctx.writeAndFlush(RespRequestHandler.stringToByteBuf("-ERR Server Error Encountered: " + cause.getMessage() + "\r\n", ctx.alloc()));
      ctx.close();
   }
}
