package org.infinispan.server.router.router.impl.singleport;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.regex.Pattern;

import org.infinispan.server.core.transport.AccessControlFilter;
import org.infinispan.server.resp.RespServer;
import org.infinispan.server.router.logging.RouterLogger;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

/**
 * Detect RESP connections
 */
public class RespDetector extends ByteToMessageDecoder {
   public static final String NAME = "resp-detector";
   private final RespServer respServer;
   private static final Pattern RESP3_HANDSHAKE = Pattern.compile("^(?is)\\*[1-9]\r\n\\$[1-9]\r\n(HELLO|AUTH|COMMAND)\r\n.*");

   public RespDetector(RespServer respServer) {
      this.respServer = respServer;
   }

   @Override
   protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
      // We need to only see the RESP HELLO message
      if (in.readableBytes() < 5) {
         // noop, wait for further reads
         return;
      }
      int i = in.readerIndex();
      // RESP commands start with * to symbolize a STRING sequence
      if (in.getByte(i) == 42) {
         // The Redis CLI sends commands as array. Accept it only if it is a HELLO or AUTH command
         CharSequence handshake = in.getCharSequence(i, in.readableBytes(), StandardCharsets.US_ASCII);
         if (RESP3_HANDSHAKE.matcher(handshake).matches()) {
            installRespHandler(ctx);
         } else {
            out.add("-ERR Only RESP3 supported\r\n");
         }
      } else if (in.getCharSequence(i, 5, StandardCharsets.US_ASCII).equals("HELLO")) {
         installRespHandler(ctx);
      }
      // Trigger any protocol-specific rules
      ctx.pipeline().fireUserEventTriggered(AccessControlFilter.EVENT);
      // Remove this
      ctx.pipeline().remove(this);
   }

   private void installRespHandler(ChannelHandlerContext ctx) {
      // We found the RESP handshake, let's do some pipeline surgery
      ChannelHandlerAdapter dummyHandler = new ChannelHandlerAdapter() {};
      ctx.pipeline().addAfter(NAME, "dummy", dummyHandler);
      ChannelHandler channelHandler = ctx.pipeline().removeLast();
      // Remove everything else
      while (channelHandler != dummyHandler) {
         channelHandler = ctx.pipeline().removeLast();
      }
      // Add the RESP server handler
      ctx.pipeline().addLast(respServer.getInitializer());
      RouterLogger.SERVER.tracef("Detected RESP connection %s", ctx);
   }
}
