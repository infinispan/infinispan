package org.infinispan.client.hotrod.event.impl;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.infinispan.client.hotrod.impl.transport.netty.ChannelRecord;
import org.infinispan.client.hotrod.impl.transport.netty.HintedReplayingDecoder;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.commons.util.Util;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;

abstract class EventDispatcher<T> extends HintedReplayingDecoder<Void> {
   static final Log log = LogFactory.getLog(EventDispatcher.class);
   static final boolean trace = log.isTraceEnabled();
   static final AtomicReferenceFieldUpdater<EventDispatcher, DispatcherStatus> statusUpdater
         = AtomicReferenceFieldUpdater.newUpdater(EventDispatcher.class, DispatcherStatus.class, "status");

   final ClientListenerNotifier notifier;
   final Object listener;
   final byte[] listenerId;
   final Channel channel;
   final String cacheName;
   volatile DispatcherStatus status = DispatcherStatus.STOPPED;

   EventDispatcher(ClientListenerNotifier notifier, String cacheName, Object listener, byte[] listenerId, Channel channel) {
      this.notifier = notifier;
      this.listener = listener;
      this.listenerId = listenerId;
      this.channel = channel;
      this.cacheName = cacheName;
   }

   @Override
   protected void decode(ChannelHandlerContext ctx, ByteBuf buf, List<Object> out) throws Exception {
      T event;
      try {
         event = readEvent(buf);
      } catch (Exception e) {
         log.unableToReadEventFromServer(e, channel.remoteAddress());
         if (statusUpdater.compareAndSet(this, DispatcherStatus.RUNNING, DispatcherStatus.STOPPED)) {
            ctx.pipeline().remove(this);
            ctx.close();
            notifier.failoverClientListener(new WrappedByteArray(listenerId));
         }
         return;
      }
      try {
         invokeEvent(event);
      } catch (Throwable t) {
         log.unexpectedErrorConsumingEvent(event, t);
      }
   }

   public abstract CompletableFuture<Short> executeFailover();

   protected abstract T readEvent(ByteBuf in);

   protected abstract void invokeEvent(T event);

   @Override
   public void channelInactive(ChannelHandlerContext ctx) throws Exception {
      if (statusUpdater.compareAndSet(this, DispatcherStatus.RUNNING, DispatcherStatus.STOPPED)) {
         log.debugf("Connection to %s reset by peer, so failover client listener %s",
               ChannelRecord.of(channel).getUnresolvedAddress(), Util.printArray(listenerId));
         notifier.failoverClientListener(new WrappedByteArray(listenerId));
      }
   }

   @Override
   public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
      log.unrecoverableErrorReadingEvent(cause, channel.remoteAddress(), Util.printArray(listenerId));
      if (statusUpdater.compareAndSet(this, DispatcherStatus.RUNNING, DispatcherStatus.STOPPED)) {
         ctx.pipeline().remove(this);
         ctx.close();
         notifier.channelFactory().releaseChannel(channel);
      }
   }

   protected void invokeFailoverEvent() {}

   enum DispatcherStatus {
      STOPPED,
      RUNNING
   }
}
