package org.infinispan.server.resp;

import java.lang.invoke.MethodHandles;
import java.util.List;

import org.infinispan.commons.logging.LogFactory;
import org.infinispan.server.resp.logging.Log;

import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.output.PushOutput;
import io.lettuce.core.protocol.RedisStateMachine;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

public class RespLettuceHandler extends ByteToMessageDecoder {
   private final static Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass(), Log.class);

   private final RedisStateMachine stateMachine = new RedisStateMachine(ByteBufAllocator.DEFAULT);
   private RespRequestHandler requestHandler;

   public RespLettuceHandler(RespServer respServer) {
      this.requestHandler = respServer.newHandler();
   }

   @Override
   protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
      PushOutput<byte[], byte[]> pushOutput = new PushOutput<>(ByteArrayCodec.INSTANCE);
      if (stateMachine.decode(in, pushOutput)) {
         String type = pushOutput.getType();
         List content = pushOutput.getContent();
         requestHandler = requestHandler.handleRequest(ctx, type, content.subList(1, content.size()));
      }
   }

   @Override
   public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      log.unexpectedException(cause);
      ctx.writeAndFlush(RespRequestHandler.stringToByteBuf("-ERR Server Error Encountered: " + cause.getMessage() + "\r\n", ctx.alloc()));
      ctx.close();
   }
}
