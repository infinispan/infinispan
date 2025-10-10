package org.infinispan.server.memcached.text;

import static org.infinispan.server.memcached.text.TextConstants.STORED;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.server.core.security.UsernamePasswordAuthenticator;
import org.infinispan.server.memcached.MemcachedBaseDecoder;
import org.infinispan.server.memcached.MemcachedInboundAdapter;
import org.infinispan.server.memcached.MemcachedResponse;
import org.infinispan.server.memcached.MemcachedServer;

/**
 * @since 15.0
 **/
public abstract class TextAuthDecoder extends TextDecoder {
   private static final CompletionStage<byte[]> FAILED_AUTH = CompletableFuture.failedFuture(new SecurityException("Wrong credentials"));

   private final UsernamePasswordAuthenticator authenticator;

   protected TextAuthDecoder(MemcachedServer server) {
      super(server, ANONYMOUS);
      authenticator = server.getConfiguration().authentication().text().authenticator();
   }

   protected final MemcachedResponse auth(TextHeader header, byte[] token) {
      return send(header, auth(token));
   }

   private CompletionStage<byte[]> auth(byte[] token) {
      String s = new String(token, StandardCharsets.US_ASCII);
      String[] parts = s.split(" ");
      if (parts.length != 2) {
         return FAILED_AUTH;
      }

      return CompletionStages.handleAndCompose(authenticator.authenticate(parts[0], parts[1].toCharArray()), (subject, t) -> {
         if (t != null) {
            return CompletableFuture.failedFuture(new SecurityException(t.getMessage()));
         }
         CompletableFuture<byte[]> cs = new CompletableFuture<>();
         ctx.channel().eventLoop().submit(() -> {
            MemcachedInboundAdapter inbound = ctx.pipeline().get(MemcachedInboundAdapter.class);
            MemcachedBaseDecoder decoder = new TextOpDecoderImpl(server, subject);
            decoder.registerExceptionHandler(inbound::handleExceptionally);
            ctx.pipeline().replace("decoder", "decoder", decoder);
            cs.complete(STORED);
         });
         return cs;
      });
   }
}
