package org.infinispan.server.resp.commands.connection;

import static org.infinispan.server.resp.RespConstants.CRLF_STRING;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.util.Version;
import org.infinispan.server.resp.ByteBufPool;
import org.infinispan.server.resp.ByteBufferUtils;
import org.infinispan.server.resp.Resp3AuthHandler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespErrorUtil;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.AuthResp3Command;

import io.netty.channel.ChannelHandlerContext;
import io.netty.util.CharsetUtil;

/**
 * `<code>HELLO [protover [AUTH username password] [SETNAME clientname]]</code>` command.
 * <p>
 * Issued by the client when establishing a new connection. This implementation only allows for RESP3 version. Any
 * other version receives an error reply.
 * </p>
 * <p>
 * If authentication is enabled in Infinispan, this command <b>must</b> be accompanied by the `<code>AUTH</code>` operation.
 * Otherwise, an error is returned.
 * </p>
 * <p>
 * The `<code>SETNAME</code>` operation is ignored.
 * </p>
 *
 * @since 14.0
 * @see <a href="https://redis.io/commands/hello/">Redis Documentation</a>
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
         return handler.myStage();
      }

      if (arguments.size() == 4) {
         successStage = handler.performAuth(ctx, arguments.get(2), arguments.get(3));
      } else if (!handler.isAuthorized() && handler.canUseCertAuth()) {
         successStage = handler.performAuth(ctx);
      } else {
         // In case authentication is enabled, HELLO must provide the additional arguments to perform the authentication.
         // A similar behavior of running with `--requirepass <password>`.
         if (!handler.isAuthorized()) {
            ByteBufferUtils.stringToByteBufAscii("-NOAUTH HELLO must be called with the client already authenticated, otherwise the HELLO <proto> AUTH <user> <pass> option can be used to authenticate the client and select the RESP protocol version at the same time\r\n", handler.allocator());
         } else {
            helloResponse(handler.allocator());
         }
      }

      if (successStage != null) {
         return handler.stageToReturn(successStage, ctx, success -> {
            if (success) helloResponse(handler.allocator());
            else RespErrorUtil.unauthorized(handler.allocator());

            return AUTH.silentCreateAfterAuthentication(success, handler);
         });
      }

      return handler.myStage();
   }

   private static void helloResponse(ByteBufPool alloc) {
      // For better compatibility with different clients, we stick the version to returning only numbers and dots.
      // Returning only X.Y or X.Y.Z
      String versionString = Version.getMajorMinor();
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
