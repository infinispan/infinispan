package org.infinispan.server.memcached.text;

import static org.infinispan.server.memcached.text.TextConstants.CLIENT_ERROR_AUTH;
import static org.infinispan.server.memcached.text.TextConstants.STORED;

import java.nio.charset.StandardCharsets;

import org.infinispan.server.core.security.UsernamePasswordAuthenticator;
import org.infinispan.server.memcached.MemcachedBaseDecoder;
import org.infinispan.server.memcached.MemcachedInboundAdapter;
import org.infinispan.server.memcached.MemcachedServer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;

/**
 * @since 15.0
 **/
public abstract class TextAuthDecoder extends TextDecoder {

   private final UsernamePasswordAuthenticator authenticator;

   protected TextAuthDecoder(MemcachedServer server) {
      super(server, ANONYMOUS);
      authenticator = server.getConfiguration().authentication().text().authenticator();
   }

   protected void auth(byte[] token) {
      String s = new String(token, StandardCharsets.US_ASCII);
      String[] parts = s.split(" ");
      if (parts.length != 2) {
         authFailure("Wrong credentials");
      } else {
         authenticator.authenticate(parts[0], parts[1].toCharArray()).handle((subject, t) -> {
            if (t != null) {
               authFailure(t.getMessage());
            } else {
               ctx.channel().eventLoop().submit(() -> {
                  ByteBuf buf = MemcachedInboundAdapter.getAllocator(ctx).acquire(STORED.length);
                  buf.writeBytes(STORED);

                  MemcachedInboundAdapter inbound = ctx.pipeline().get(MemcachedInboundAdapter.class);
                  MemcachedBaseDecoder decoder = new TextOpDecoderImpl(server, subject);
                  decoder.registerExceptionHandler(inbound::handleExceptionally);
                  ctx.pipeline().replace("decoder", "decoder", decoder);

                  inbound.flushBufferIfNeeded(ctx);
               });
            }
            return null;
         });
      }
   }

   private void authFailure(String message) {
      ctx.channel().eventLoop().submit(() -> {
         String s = CLIENT_ERROR_AUTH + message;
         ByteBuf buf = MemcachedInboundAdapter.getAllocator(ctx).acquire(s.length());
         ByteBufUtil.writeAscii(buf, s);
      });
   }
}
