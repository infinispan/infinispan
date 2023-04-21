package org.infinispan.server.resp;

import io.netty.channel.ChannelHandlerContext;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.server.resp.commands.AuthResp3Command;

import javax.security.auth.Subject;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletionStage;

public class Resp3AuthHandler extends CacheRespRequestHandler {

   public Resp3AuthHandler(RespServer server) {
      super(server);
   }

   @Override
   protected CompletionStage<RespRequestHandler> actualHandleRequest(ChannelHandlerContext ctx, RespCommand type, List<byte[]> arguments) {
      if (type instanceof AuthResp3Command) {
         AuthResp3Command type1 = (AuthResp3Command) type;
         return type1.perform(this, ctx, arguments);
      }

      if (isAuthorized()) {
         return super.actualHandleRequest(ctx, type, arguments);
      } else {
         handleUnauthorized(ctx);
      }

      return myStage;
   }

   public CompletionStage<Boolean> performAuth(ChannelHandlerContext ctx, byte[] username, byte[] password) {
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
         ByteBufferUtils.stringToByteBuf("-ERR Client sent AUTH, but no password is set\r\n", allocatorToUse);
         return false;
      }

      setCache(cache.withSubject(subject));
      Consumers.OK_BICONSUMER.accept(null, allocatorToUse);
      return true;
   }

   private void handleUnauthorized(ChannelHandlerContext ctx) {
      assert ctx.channel().eventLoop().inEventLoop();
      ByteBufferUtils.stringToByteBuf("-WRONGPASS invalid username-password pair or user is disabled.\r\n", allocatorToUse);
   }

   private boolean isAuthorized() {
      return this.getClass() != Resp3AuthHandler.class;
   }

}
