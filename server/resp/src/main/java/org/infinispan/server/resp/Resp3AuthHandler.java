package org.infinispan.server.resp;

import static org.infinispan.server.resp.Resp3Handler.statusOK;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletionStage;

import javax.security.auth.Subject;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.util.Version;
import org.infinispan.commons.util.concurrent.CompletableFutures;

import io.netty.channel.ChannelHandlerContext;
import io.netty.util.CharsetUtil;

public class Resp3AuthHandler extends RespRequestHandler {

   protected final RespServer respServer;
   protected AdvancedCache<byte[], byte[]> cache;

   public Resp3AuthHandler(RespServer server) {
      this.respServer = server;
      this.cache = server.getCache();
   }

   @Override
   public CompletionStage<RespRequestHandler> handleRequest(ChannelHandlerContext ctx, String type, List<byte[]> arguments) {
      CompletionStage<Boolean> successStage = null;
      switch (type) {
         case "HELLO":
            byte[] respProtocolBytes = arguments.get(0);
            String version = new String(respProtocolBytes, CharsetUtil.UTF_8);
            if (!version.equals("3")) {
               ctx.writeAndFlush(RespRequestHandler.stringToByteBuf("-NOPROTO sorry this protocol version is not supported\r\n", ctx.alloc()));
               break;
            }

            if (arguments.size() == 4) {
               successStage = performAuth(ctx, arguments.get(2), arguments.get(3));
            } else {
               helloResponse(ctx);
            }
            break;
         case "AUTH":
            successStage = performAuth(ctx, arguments.get(0), arguments.get(1));
            break;
         case "QUIT":
            ctx.close();
            break;
         default:
            if (isAuthorized()) super.handleRequest(ctx, type, arguments);
            else handleUnauthorized(ctx);
      }

      if (successStage != null) {
         return stageToReturn(successStage, ctx,
               auth -> auth ? respServer.newHandler() : this);
      }

      return myStage;
   }

   private CompletionStage<Boolean> performAuth(ChannelHandlerContext ctx, byte[] username, byte[] password) {
      return performAuth(ctx, new String(username, StandardCharsets.UTF_8), new String(password, StandardCharsets.UTF_8));
   }

   private CompletionStage<Boolean> performAuth(ChannelHandlerContext ctx, String username, String password) {
      Authenticator authenticator = respServer.getConfiguration().authentication().authenticator();
      if (authenticator == null) {
         return CompletableFutures.booleanStage(handleAuthResponse(ctx, null));
      }
      return authenticator.authenticate(username, password.toCharArray())
            // Note we have to write to our variables in the event loop (in this case cache)
            .thenApplyAsync(r -> handleAuthResponse(ctx, r), ctx.channel().eventLoop())
            .exceptionally(t -> {
               handleUnauthorized(ctx);
               return false;
            });
   }

   private boolean handleAuthResponse(ChannelHandlerContext ctx, Subject subject) {
      assert ctx.channel().eventLoop().inEventLoop();
      if (subject == null) {
         ctx.writeAndFlush(RespRequestHandler.stringToByteBuf("-ERR Client sent AUTH, but no password is set\r\n", ctx.alloc()));
         return false;
      }

      cache = cache.withSubject(subject);
      ctx.writeAndFlush(statusOK());
      return true;
   }

   private void handleUnauthorized(ChannelHandlerContext ctx) {
      assert ctx.channel().eventLoop().inEventLoop();
      ctx.writeAndFlush(RespRequestHandler.stringToByteBuf("-WRONGPASS invalid username-password pair or user is disabled.\r\n", ctx.alloc()));
   }

   private boolean isAuthorized() {
      return this.getClass() != Resp3AuthHandler.class;
   }

   private static void helloResponse(ChannelHandlerContext ctx) {
      String versionString = Version.getBrandVersion();
      ctx.writeAndFlush(RespRequestHandler.stringToByteBuf("%7\r\n" +
            "$6\r\nserver\r\n$15\r\nInfinispan RESP\r\n" +
            "$7\r\nversion\r\n$" + versionString.length() + "\r\n" + versionString + "\r\n" +
            "$5\r\nproto\r\n:3\r\n" +
            "$2\r\nid\r\n:184\r\n" +
            "$4\r\nmode\r\n$7\r\ncluster\r\n" +
            "$4\r\nrole\r\n$6\r\nmaster\r\n" +
            "$7\r\nmodules\r\n*0\r\n", ctx.alloc()));
   }
}
