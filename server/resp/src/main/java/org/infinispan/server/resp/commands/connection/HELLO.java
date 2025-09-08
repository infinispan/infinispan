package org.infinispan.server.resp.commands.connection;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.util.Version;
import org.infinispan.server.core.transport.ConnectionMetadata;
import org.infinispan.server.resp.Resp3AuthHandler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespErrorUtil;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.AuthResp3Command;
import org.infinispan.server.resp.exception.RespCommandException;
import org.infinispan.server.resp.serialization.ByteBufferUtils;
import org.infinispan.server.resp.serialization.Resp3Response;
import org.infinispan.server.resp.serialization.RespConstants;

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
      CompletionStage<Void> successStage = null;

      byte[] respProtocolBytes = arguments.get(0);
      String version = new String(respProtocolBytes, CharsetUtil.UTF_8);
      if (!version.equals("3")) {
         RespErrorUtil.customRawError("-NOPROTO sorry this protocol version is not supported", handler.allocator());
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
            return CompletableFuture.failedFuture(new RespCommandException("NOAUTH HELLO must be called with the client already authenticated, otherwise the HELLO <proto> AUTH <user> <pass> option can be used to authenticate the client and select the RESP protocol version at the same time"));
         } else {
            helloResponse(handler, ctx);
         }
      }

      if (successStage != null) {
         return handler.stageToReturn(successStage, ctx, success -> {
            RespRequestHandler next = AUTH.silentCreateAfterAuthentication(handler);
            if (next == null)
               return handler;

            helloResponse(handler, ctx);
            return next;
         });
      }

      return handler.myStage();
   }

   private static void helloResponse(Resp3AuthHandler handler, ChannelHandlerContext ctx) {
      // For better compatibility with different clients, we stick the version to returning only numbers and dots.
      // Returning only X.Y or X.Y.Z
      String versionString = Version.getMajorMinor();
      ConnectionMetadata metadata = ConnectionMetadata.getInstance(ctx.channel());

      // Map mixes different types.
      Resp3Response.write(handler.allocator(), (ignore, alloc) -> {
         ByteBufferUtils.writeNumericPrefix(RespConstants.MAP, 7, alloc);

         Resp3Response.simpleString("server", alloc);
         Resp3Response.simpleString("Infinispan RESP", alloc);

         Resp3Response.simpleString("version", alloc);
         Resp3Response.simpleString(versionString, alloc);

         Resp3Response.simpleString("proto", alloc);
         Resp3Response.integers(3, alloc);

         Resp3Response.simpleString("id", alloc);
         Resp3Response.integers(metadata.id(), alloc);

         Resp3Response.simpleString("mode", alloc);
         Resp3Response.simpleString("cluster", alloc);

         Resp3Response.simpleString("role", alloc);
         Resp3Response.simpleString("master", alloc);

         Resp3Response.simpleString("modules", alloc);
         Resp3Response.arrayEmpty(alloc);
      });
   }
}
