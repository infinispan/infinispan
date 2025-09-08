package org.infinispan.server.resp;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import javax.security.auth.Subject;
import javax.security.sasl.SaslException;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.server.core.transport.ConnectionMetadata;
import org.infinispan.server.resp.authentication.RespAuthenticator;
import org.infinispan.server.resp.commands.AuthResp3Command;

import io.netty.channel.ChannelHandlerContext;

public class Resp3AuthHandler extends CacheRespRequestHandler {

   public Resp3AuthHandler(RespServer server) {
      this(server, server.getCache());
   }

   protected Resp3AuthHandler(RespServer server, AdvancedCache<byte[], byte[]> cache) {
      super(server, cache);
   }

   @Override
   protected CompletionStage<RespRequestHandler> actualHandleRequest(ChannelHandlerContext ctx, RespCommand command, List<byte[]> arguments) {
      if (command instanceof AuthResp3Command authCommand) {
         return authCommand.perform(this, ctx, arguments);
      }

      if (isAuthorized()) {
         return super.actualHandleRequest(ctx, command, arguments);
      } else {
         handleUnauthorized(ctx);
      }

      return myStage;
   }

   public CompletionStage<Void> performAuth(ChannelHandlerContext ctx, byte[] username, byte[] password) {
      return performAuth(ctx, new String(username, StandardCharsets.UTF_8), new String(password, StandardCharsets.UTF_8));
   }

   public CompletionStage<Void> performAuth(ChannelHandlerContext ctx) {
      return performAuth(ctx, (String) null, null);
   }

   private CompletionStage<Void> performAuth(ChannelHandlerContext ctx, String username, String password) {
      RespAuthenticator authenticator = respServer.getConfiguration().authentication().authenticator();
      if (authenticator == null) {
         return CompletableFuture.failedFuture(new SecurityException("invalid username-password pair or user is disabled"));
      }

      CompletionStage<Subject> authentication;
      if (username == null && password == null) {
         try {
            authentication = canUseCertAuth()
                  ? authenticator.clientCertAuth(ctx.channel())
                  : CompletableFutures.completedNull();
         } catch (SaslException e) {
            return CompletableFuture.failedFuture(e);
         }
      } else {
         authentication = authenticator.usernamePasswordAuth(username, password.toCharArray());
      }

      return authentication
            // Note we have to write to our variables in the event loop (in this case cache)
            .thenAcceptAsync(r -> handleAuthResponse(ctx, r), ctx.channel().eventLoop());
   }

   private void handleAuthResponse(ChannelHandlerContext ctx, Subject subject) {
      assert ctx.channel().eventLoop().inEventLoop();
      if (subject == null) {
         throw new SecurityException("invalid username-password pair or user is disabled");
      }
      ConnectionMetadata metadata = ConnectionMetadata.getInstance(ctx.channel());
      metadata.subject(subject);
      setCache(cache().withSubject(subject));
   }

   private void handleUnauthorized(ChannelHandlerContext ctx) {
      assert ctx.channel().eventLoop().inEventLoop();
      writer().unauthorized();
   }

   public boolean isAuthorized() {
      return this.getClass() != Resp3AuthHandler.class;
   }

   public boolean canUseCertAuth() {
      RespAuthenticator authenticator = respServer.getConfiguration().authentication().authenticator();
      return authenticator != null && authenticator.isClientCertAuthEnabled();
   }
}
