package org.infinispan.server.memcached.text;

import java.util.concurrent.CompletionStage;

import javax.security.auth.Subject;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.server.memcached.MemcachedBaseDecoder;
import org.infinispan.server.memcached.MemcachedServer;
import org.infinispan.server.memcached.logging.Header;

import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

/**
 * @since 15.0
 **/
abstract class TextDecoder extends MemcachedBaseDecoder {
   protected TextDecoder(MemcachedServer server, Subject subject) {
      super(server, subject, server.getCache().getAdvancedCache().withMediaType(MediaType.TEXT_PLAIN, server.getConfiguration().clientEncoding()));
   }

   @Override
   public void send(Header header, CompletionStage<?> response) {
      new TextResponse(current, channel).queueResponse(accessLogging ? header : null, response);
   }

   @Override
   public void send(Header header, CompletionStage<?> response, GenericFutureListener<? extends Future<? super Void>> listener) {
      new TextResponse(current, channel).queueResponse(accessLogging ? header : null, response, listener);
   }
}
