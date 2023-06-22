package org.infinispan.server.memcached.text;

import java.util.concurrent.CompletionStage;

import javax.security.auth.Subject;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.server.core.transport.ConnectionMetadata;
import org.infinispan.server.memcached.MemcachedBaseDecoder;
import org.infinispan.server.memcached.MemcachedResponse;
import org.infinispan.server.memcached.MemcachedServer;
import org.infinispan.server.memcached.logging.Header;

import io.netty.channel.ChannelHandlerContext;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

/**
 * @since 15.0
 **/
abstract class TextDecoder extends MemcachedBaseDecoder {

   protected TokenReader reader;

   protected TextDecoder(MemcachedServer server, Subject subject) {
      super(server, subject, server.getCache().getAdvancedCache().withMediaType(MediaType.TEXT_PLAIN, server.getConfiguration().clientEncoding()));
   }

   @Override
   public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
      super.handlerAdded(ctx);

      // It can exceed the 256 for large values, so we don't have a max capacity.
      this.reader = new TokenReader(ctx.alloc().buffer(256));
      ConnectionMetadata metadata = ConnectionMetadata.getInstance(ctx.channel());
      metadata.subject(subject);
      metadata.protocolVersion("MCTXT");
   }

   @Override
   public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
      super.channelUnregistered(ctx);
      if (this.reader != null) reader.release();
   }

   @Override
   protected MemcachedResponse failedResponse(Header header, Throwable t) {
      return new TextResponse(t, header);
   }

   @Override
   protected MemcachedResponse send(Header header, CompletionStage<?> response) {
      return new TextResponse(response, header, null);
   }

   @Override
   protected MemcachedResponse send(Header header, CompletionStage<?> response, GenericFutureListener<? extends Future<? super Void>> listener) {
      return new TextResponse(response, header, listener);
   }
}
