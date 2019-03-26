package org.infinispan.server.hotrod;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;

import org.infinispan.commons.logging.LogFactory;
import org.infinispan.counter.EmbeddedCounterManagerFactory;
import org.infinispan.counter.impl.manager.EmbeddedCounterManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.core.ServerConstants;
import org.infinispan.server.hotrod.logging.Log;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.DecoderException;

abstract class BaseDecoder extends ByteToMessageDecoder {
   protected final static Log log = LogFactory.getLog(BaseDecoder.class, Log.class);
   protected final static boolean trace = log.isTraceEnabled();

   protected final EmbeddedCacheManager cacheManager;
   protected final Executor executor;
   protected final HotRodServer server;

   protected Authentication auth;
   protected TransactionRequestProcessor cacheProcessor;
   protected CounterRequestProcessor counterProcessor;
   protected MultimapRequestProcessor multimapProcessor;
   protected TaskRequestProcessor taskProcessor;

   protected BaseDecoder(EmbeddedCacheManager cacheManager, Executor executor, HotRodServer server) {
      this.cacheManager = cacheManager;
      this.executor = executor;
      this.server = server;
   }

   public Executor getExecutor() {
      return executor;
   }

   @Override
   public void handlerAdded(ChannelHandlerContext ctx) {
      auth = new Authentication(ctx.channel(), executor, server);
      cacheProcessor = new TransactionRequestProcessor(ctx.channel(), executor, server);
      counterProcessor = new CounterRequestProcessor(ctx.channel(), (EmbeddedCounterManager) EmbeddedCounterManagerFactory.asCounterManager(cacheManager), executor, server);
      multimapProcessor = new MultimapRequestProcessor(ctx.channel(), executor, server);
      taskProcessor = new TaskRequestProcessor(ctx.channel(), executor, server);
   }

   @Override
   public void channelActive(ChannelHandlerContext ctx) throws Exception {
      super.channelActive(ctx);
      if (trace) {
         log.tracef("Channel %s became active", ctx.channel());
      }
      server.getClientListenerRegistry().findAndWriteEvents(ctx.channel());
      server.getClientCounterNotificationManager().channelActive(ctx.channel());
   }

   @Override
   public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
      super.channelWritabilityChanged(ctx);
      Channel channel = ctx.channel();
      boolean writeable = channel.isWritable();
      if (trace) {
         log.tracef("Channel %s writability changed to %b", channel, writeable);
      }
      if (writeable) {
         server.getClientListenerRegistry().findAndWriteEvents(channel);
         server.getClientCounterNotificationManager().channelActive(channel);
      }
   }

   @Override
   public void exceptionCaught(ChannelHandlerContext ctx, Throwable t) throws Exception {
      log.debug("Exception caught", t);
      if (t instanceof DecoderException) {
         t = t.getCause();
      }
      cacheProcessor.writeException(getHeader(), t);
   }

   protected abstract HotRodHeader getHeader();

   protected int defaultExpiration(int duration, int flags, ProtocolFlag defaultFlag) {
      if (duration > 0) return duration;
      return (flags & defaultFlag.getValue()) != 0 ? ServerConstants.EXPIRATION_DEFAULT : ServerConstants.EXPIRATION_NONE;
   }

   /**
    * We usually know the size of the map ahead, and we want to return static empty map if we're not going to add anything.
    */
   protected <K, V> Map<K, V> allocMap(int size) {
      return size == 0 ? Collections.emptyMap() : new HashMap<>(size);
   }

   protected <T> List<T> allocList(int size) {
      return size == 0 ? Collections.emptyList() : new ArrayList<>(size);
   }

   protected <T> Set<T> allocSet(int size) {
      return size == 0 ? Collections.emptySet() : new HashSet<>(size);
   }
}
