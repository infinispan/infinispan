package org.infinispan.server.resp.commands.connection;

import static org.infinispan.server.resp.RespConstants.CRLF_STRING;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.util.Version;
import org.infinispan.server.resp.ByteBufPool;
import org.infinispan.server.resp.ByteBufferUtils;
import org.infinispan.server.resp.Resp3AuthHandler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.AuthResp3Command;

import io.netty.channel.ChannelHandlerContext;
import io.netty.util.CharsetUtil;

/**
 * @link https://redis.io/commands/hello/
 * @since 14.0
 */
public class HELLO extends RespCommand implements AuthResp3Command {
   public HELLO() {
      super(-1, 0, 0,0);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3AuthHandler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      CompletionStage<Boolean> successStage = null;

      byte[] respProtocolBytes = arguments.get(0);
      String version = new String(respProtocolBytes, CharsetUtil.UTF_8);
      if (!version.equals("3")) {
         ByteBufferUtils.stringToByteBufAscii("-NOPROTO sorry this protocol version is not supported\r\n", handler.allocator());
      } else {
         if (arguments.size() == 4) {
            successStage = handler.performAuth(ctx, arguments.get(2), arguments.get(3));
         } else if (!handler.isAuthorized() && handler.canUseCertAuth()) {
            successStage = handler.performAuth(ctx);
         } else {
            helloResponse(handler.allocator());
         }
      }

      if (successStage != null) {
         return handler.stageToReturn(successStage, ctx, success -> AUTH.createAfterAuthentication(success, handler));
      }

      return handler.myStage();
   }

   private static void helloResponse(ByteBufPool alloc) {
      String versionString = Version.getBrandVersion();
      ByteBufferUtils.stringToByteBufAscii("%7\r\n" +
            "$6\r\nserver\r\n$15\r\nInfinispan RESP\r\n" +
            "$7\r\nversion\r\n$" + versionString.length() + CRLF_STRING + versionString + CRLF_STRING +
            "$5\r\nproto\r\n:3\r\n" +
            "$2\r\nid\r\n:184\r\n" +
            "$4\r\nmode\r\n$7\r\ncluster\r\n" +
            "$4\r\nrole\r\n$6\r\nmaster\r\n" +
            "$7\r\nmodules\r\n*0\r\n", alloc);
   }
}
