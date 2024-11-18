package org.infinispan.server.resp.commands.connection;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.util.Version;
import org.infinispan.server.core.transport.ConnectionMetadata;
import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3AuthHandler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.AuthResp3Command;
import org.infinispan.server.resp.serialization.RespConstants;
import org.infinispan.server.resp.serialization.ResponseWriter;

import io.netty.channel.ChannelHandlerContext;
import io.netty.util.CharsetUtil;

/**
 * HELLO
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
 * @see <a href="https://redis.io/commands/hello/">HELLO</a>
 * @since 14.0
 */
public class HELLO extends RespCommand implements AuthResp3Command {
   public HELLO() {
      super(-1, 0, 0, 0);
   }

   @Override
   public long aclMask() {
      return AclCategory.FAST | AclCategory.CONNECTION;
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3AuthHandler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      CompletionStage<Boolean> successStage = null;

      byte[] respProtocolBytes = arguments.get(0);
      String version = new String(respProtocolBytes, CharsetUtil.UTF_8);
      if (!version.equals("3")) {
         handler.writer().error("-NOPROTO sorry this protocol version is not supported");
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
            handler.writer().error("-NOAUTH HELLO must be called with the client already authenticated, otherwise the HELLO <proto> AUTH <user> <pass> option can be used to authenticate the client and select the RESP protocol version at the same time");
         } else {
            helloResponse(handler, ctx);
         }
      }

      if (successStage != null) {
         return handler.stageToReturn(successStage, ctx, success -> {
            RespRequestHandler next = AUTH.silentCreateAfterAuthentication(success, handler);
            if (next == null)
               return handler;

            if (success) helloResponse(handler, ctx);
            else handler.writer().unauthorized();
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
      ResponseWriter writer = handler.writer();

      writer.writeNumericPrefix(RespConstants.MAP, 7);

      writer.simpleString("server");
      writer.simpleString("Infinispan RESP");

      writer.simpleString("version");
      writer.simpleString(versionString);

      writer.simpleString("proto");
      writer.integers(3);

      writer.simpleString("id");
      writer.integers(metadata.id());

      writer.simpleString("mode");
      writer.simpleString("cluster");

      writer.simpleString("role");
      writer.simpleString("master");

      writer.simpleString("modules");
      writer.arrayEmpty();
   }
}
