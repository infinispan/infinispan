package org.infinispan.server.memcached.text;

import static org.infinispan.server.memcached.text.TextConstants.CLIENT_ERROR_AUTH;
import static org.infinispan.server.memcached.text.TextConstants.STORED;

import java.nio.charset.StandardCharsets;

import org.infinispan.server.core.security.UsernamePasswordAuthenticator;
import org.infinispan.server.memcached.MemcachedServer;

import io.netty.buffer.ByteBuf;

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
               channel.eventLoop().submit(() -> {
                  ByteBuf buf = channel.alloc().ioBuffer();
                  buf.writeBytes(STORED);
                  channel.writeAndFlush(buf);
                  channel.pipeline().replace("decoder", "decoder", new TextOpDecoderImpl(server, subject));
               });
            }
            return null;
         });
      }
   }

   private void authFailure(String message) {
      channel.eventLoop().submit(() -> {
         ByteBuf buf = channel.alloc().ioBuffer();
         buf.writeCharSequence(CLIENT_ERROR_AUTH + message, StandardCharsets.US_ASCII);
         channel.writeAndFlush(buf);
      });
   }
}
