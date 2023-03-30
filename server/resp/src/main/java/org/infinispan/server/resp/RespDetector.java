package org.infinispan.server.resp;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.regex.Pattern;

import org.infinispan.server.core.ProtocolDetector;
import org.infinispan.server.core.transport.AccessControlFilter;
import org.infinispan.server.resp.logging.Log;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

/**
 * Detect RESP connections
 */
public class RespDetector extends ProtocolDetector {
   public static final String NAME = "resp-detector";
   private static final Pattern RESP3_HANDSHAKE = Pattern.compile("^(?is)\\*[1-9]\r\n\\$[1-9]\r\n(HELLO|AUTH|COMMAND)\r\n.*");

   public RespDetector(RespServer server) {
      super(server);
   }

   @Override
   public String getName() {
      return NAME;
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
      // Remove this
      ctx.pipeline().remove(this);
   }

   private void installRespHandler(ChannelHandlerContext ctx) {
      // We found the RESP handshake, let's do some pipeline surgery
      trimPipeline(ctx);
      // Add the RESP server handler
      ctx.pipeline().addLast(server.getInitializer());
      // Make sure to fire registered on the newly installed handlers
      ctx.fireChannelRegistered();
      Log.SERVER.tracef("Detected RESP connection %s", ctx);
      // Trigger any protocol-specific rules
      ctx.pipeline().fireUserEventTriggered(AccessControlFilter.EVENT);
   }
}
