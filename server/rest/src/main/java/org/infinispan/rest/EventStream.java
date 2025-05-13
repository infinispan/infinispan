package org.infinispan.rest;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;

import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.rest.logging.Log;

import io.netty.channel.ChannelHandlerContext;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 13.0
 **/
public class EventStream implements Closeable {
   private static final Log log = LogFactory.getLog(EventStream.class, Log.class);
   private final Consumer<EventStream> onOpen;
   private final Runnable onClose;
   private ChannelHandlerContext ctx;

   public EventStream(Consumer<EventStream> onOpen, Runnable onClose) {
      this.onOpen = onOpen;
      this.onClose = onClose;
   }

   public CompletionStage<Void> sendEvent(ServerSentEvent e) {
      if (ctx != null) {
         log.tracef("Sending event: %s", e);
         CompletableFuture<Void> cf = new CompletableFuture<>();
         ctx.writeAndFlush(e).addListener(v -> cf.complete(null));
         return cf;
      } else {
         return CompletableFutures.completedNull();
      }
   }

   @Override
   public void close() throws IOException {
      log.tracef("Closing listener");
      if (onClose != null) {
         onClose.run();
      }
   }

   public void setChannelHandlerContext(ChannelHandlerContext ctx) {
      this.ctx = ctx;
      ctx.channel().closeFuture().addListener(f -> this.close());
      if (onOpen != null) {
         onOpen.accept(this);
      }
   }
}
