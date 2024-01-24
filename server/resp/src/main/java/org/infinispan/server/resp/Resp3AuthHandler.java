package org.infinispan.server.resp;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletionStage;

import javax.security.auth.Subject;

import org.infinispan.commons.util.Version;
import org.infinispan.commons.util.concurrent.CompletableFutures;

import io.netty.channel.ChannelHandlerContext;
import io.netty.util.CharsetUtil;

public class Resp3AuthHandler extends CacheRespRequestHandler {

   public Resp3AuthHandler(RespServer server) {
      super(server);
   }

   @Override
   protected CompletionStage<RespRequestHandler> actualHandleRequest(ChannelHandlerContext ctx, RespCommand type, List<byte[]> arguments) {
      CompletionStage<Boolean> successStage = null;
      switch (type) {
         case HELLO:
            byte[] respProtocolBytes = arguments.get(0);
            String version = new String(respProtocolBytes, CharsetUtil.UTF_8);
            if (!version.equals("3")) {
               stringToByteBuf("-NOPROTO sorry this protocol version is not supported\r\n", allocatorToUse);
               break;
            }

            if (arguments.size() == 4) {
               successStage = performAuth(ctx, arguments.get(2), arguments.get(3));
            } else {
               if (!isAuthorized()) {
                  stringToByteBuf("-NOAUTH HELLO must be called with the client already authenticated, otherwise the HELLO <proto> AUTH <user> <pass> option can be used to authenticate the client and select the RESP protocol version at the same time\r\n", allocatorToUse);
               } else {
                  helloResponse(ctx, allocatorToUse);
               }
            }
            break;
         case AUTH:
            successStage = performAuth(ctx, arguments.get(0), arguments.get(1));
            break;
         case QUIT:
            ctx.close();
            break;
         default:
            if (isAuthorized()) super.actualHandleRequest(ctx, type, arguments);
            else handleUnauthorized(ctx);
      }

      if (successStage != null) {
         return stageToReturn(successStage, ctx, auth -> createAfterAuthentication(ctx, auth, type));
      }

      return myStage;
   }

   private RespRequestHandler createAfterAuthentication(ChannelHandlerContext ctx, boolean success, RespCommand type) {
      if (success) {
         if (type == RespCommand.HELLO) {
            helloResponse(ctx, allocatorToUse);
         } else {
            Resp3Handler.OK_BICONSUMER.accept(null, allocatorToUse);
         }
      } else handleUnauthorized(ctx);

      return success ? respServer.newHandler() : this;
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
            .exceptionally(t -> false);
   }

   private boolean handleAuthResponse(ChannelHandlerContext ctx, Subject subject) {
      assert ctx.channel().eventLoop().inEventLoop();
      if (subject == null) {
         return false;
      }

      setCache(cache.withSubject(subject));
      return true;
   }

   private void handleUnauthorized(ChannelHandlerContext ctx) {
      assert ctx.channel().eventLoop().inEventLoop();
      stringToByteBuf("-WRONGPASS invalid username-password pair or user is disabled.\r\n", allocatorToUse);
   }

   private boolean isAuthorized() {
      return this.getClass() != Resp3AuthHandler.class;
   }

   private static void helloResponse(ChannelHandlerContext ctx, ByteBufPool alloc) {
      String versionString = Version.getBrandVersion();
      RespRequestHandler.stringToByteBuf("%7\r\n" +
            "$6\r\nserver\r\n$15\r\nInfinispan RESP\r\n" +
            "$7\r\nversion\r\n$" + versionString.length() + "\r\n" + versionString + "\r\n" +
            "$5\r\nproto\r\n:3\r\n" +
            "$2\r\nid\r\n:184\r\n" +
            "$4\r\nmode\r\n$7\r\ncluster\r\n" +
            "$4\r\nrole\r\n$6\r\nmaster\r\n" +
            "$7\r\nmodules\r\n*0\r\n", alloc);
   }
}
