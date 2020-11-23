package org.infinispan.server.resp;

import java.lang.invoke.MethodHandles;
import java.util.List;

import org.infinispan.Cache;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.server.core.logging.Log;

import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.output.PushOutput;
import io.lettuce.core.protocol.RedisStateMachine;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

public class RespLettuceHandler extends ByteToMessageDecoder {
   private static Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass(), Log.class);

   private static final ByteBufAllocator ALLOCATOR = ByteBufAllocator.DEFAULT;
   private final RedisStateMachine stateMachine = new RedisStateMachine(ALLOCATOR);

   private RespRequestHandler requestHandler = Resp3Handler.getInstance();

   private final Cache<byte[], byte[]> cache;

   public RespLettuceHandler(Cache<byte[], byte[]> cache) {
      this.cache = cache;
   }

   @Override
   protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
      PushOutput<byte[], byte[]> pushOutput = new PushOutput<>(ByteArrayCodec.INSTANCE);
      if (stateMachine.decode(in, pushOutput)) {
         String type = pushOutput.getType();
         List content = pushOutput.getContent();
         requestHandler = requestHandler.handleRequest(ctx, cache, type, content.subList(1, content.size()));
      }
   }

   @Override
   public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      System.err.print("exceptionCaught: ");
      cause.printStackTrace(System.err);
      ctx.writeAndFlush(requestHandler.stringToByteBuf("-ERR Server Error Encountered: " + cause.getMessage() + "\r\n", ctx.alloc()));
      ctx.close();
   }
}
