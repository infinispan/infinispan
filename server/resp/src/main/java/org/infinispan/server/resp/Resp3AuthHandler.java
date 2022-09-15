package org.infinispan.server.resp;

import static org.infinispan.server.resp.Resp3Handler.handleThrowable;
import static org.infinispan.server.resp.Resp3Handler.statusOK;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

import javax.security.auth.Subject;

import io.netty.channel.ChannelHandlerContext;
import io.netty.util.CharsetUtil;
import org.infinispan.AdvancedCache;
import org.infinispan.commons.util.Version;
import org.infinispan.commons.util.concurrent.CompletableFutures;

public class Resp3AuthHandler implements RespRequestHandler {

   protected final RespServer respServer;
   protected AdvancedCache<byte[], byte[]> cache;

   public Resp3AuthHandler(RespServer server) {
      this.respServer = server;
      this.cache = server.getCache();
   }

   @Override
   public RespRequestHandler handleRequest(ChannelHandlerContext ctx, String type, List<byte[]> arguments) {
      boolean success = false;
      switch (type) {
         case "HELLO":
            byte[] respProtocolBytes = arguments.get(0);
            String version = new String(respProtocolBytes, CharsetUtil.UTF_8);
            if (!version.equals("3")) {
               ctx.writeAndFlush(RespRequestHandler.stringToByteBuf("-NOPROTO sorry this protocol version is not supported\r\n", ctx.alloc()));
               break;
            }

            if (arguments.size() == 4) {
               success = performAuth(ctx, arguments.get(2), arguments.get(3));
            } else {
               helloResponse(ctx);
            }
            break;
         case "AUTH":
            success = performAuth(ctx, arguments.get(0), arguments.get(1));
            break;
         case "QUIT":
            // TODO: need to close connection
            ctx.flush();
            break;
         default:
            if (isAuthorized()) RespRequestHandler.super.handleRequest(ctx, type, arguments);
            else handleUnauthorized(ctx);
      }

      return success ? respServer.newHandler() : this;
   }

   private boolean performAuth(ChannelHandlerContext ctx, byte[] username, byte[] password) {
      return performAuth(ctx, new String(username, StandardCharsets.UTF_8), new String(password, StandardCharsets.UTF_8));
   }

   private boolean performAuth(ChannelHandlerContext ctx, String username, String password) {
      Authenticator authenticator = respServer.getConfiguration().authentication().authenticator();
      if (authenticator == null) {
         return handleAuthResponse(ctx, null);
      }
      CompletionStage<Boolean> cs = authenticator.authenticate(username, password.toCharArray())
            .thenApply(r -> handleAuthResponse(ctx, r))
            .exceptionally(t -> {
               handleThrowable(ctx, t);
               return false;
            });
      try {
         return CompletableFutures.await(cs.toCompletableFuture());
      } catch (ExecutionException | InterruptedException e) {
         handleThrowable(ctx, e);
      }

      return false;
   }

   private boolean handleAuthResponse(ChannelHandlerContext ctx, Subject subject) {
      if (subject == null) {
         ctx.writeAndFlush(ctx.writeAndFlush(RespRequestHandler.stringToByteBuf("-ERR Client sent AUTH, but no password is set" + "\r\n", ctx.alloc())));
         return false;
      }

      cache = cache.withSubject(subject);
      ctx.writeAndFlush(statusOK());
      return true;
   }

   private void handleUnauthorized(ChannelHandlerContext ctx) {
      ctx.writeAndFlush(RespRequestHandler.stringToByteBuf("-ERR unauthorized command\r\n", ctx.alloc()));
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
