package org.infinispan.client.hotrod.impl.transport.netty;

import java.net.SocketAddress;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.client.hotrod.impl.operations.HotRodOperation;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.util.internal.shaded.org.jctools.queues.MessagePassingQueue;
import io.netty.util.internal.shaded.org.jctools.queues.MpscUnboundedArrayQueue;

public class V5ChannelPool implements ChannelPool, MessagePassingQueue.Consumer<ChannelOperation> {
   private static final Log log = LogFactory.getLog(V5ChannelPool.class);
   private final SocketAddress address;
   private final ChannelInitializer newChannelInvoker;
   private final AtomicBoolean attemptedConnect = new AtomicBoolean();
   private final BiConsumer<ChannelPool, ChannelFactory.ChannelEventType> connectionFailureListener;
   // Unfortunately MessagePassingQueue doesn't implement Queue so we use the concrete class
   private final MpscUnboundedArrayQueue<ChannelOperation> queue = new MpscUnboundedArrayQueue<>(128);

   private volatile Channel channel;

   // Outbound variables (only accessed in event loop for this channel)
   HeaderDecoder headerDecoder;
   ByteBuf buffer;

   public V5ChannelPool(SocketAddress address, ChannelInitializer channelInitializer,
                        BiConsumer<ChannelPool, ChannelFactory.ChannelEventType> connectionFailureListener) {
      this.address = address;
      this.newChannelInvoker = channelInitializer;
      this.connectionFailureListener = connectionFailureListener;
   }

   public static V5ChannelPool createAndStartPool(SocketAddress address, ChannelInitializer newChannelInvoker,
                                           BiConsumer<ChannelPool, ChannelFactory.ChannelEventType> connectionFailureListener) {
      V5ChannelPool channelPool = new V5ChannelPool(address, newChannelInvoker, connectionFailureListener);
      channelPool.attemptConnect();
      return channelPool;
   }

   private void attemptConnect() {
      if (!attemptedConnect.getAndSet(true)) {
         return;
      }
      newChannelInvoker.createChannel().whenComplete((c, t) -> {
         if (t != null) {
            TransportException cause = new TransportException(t, address);
            // HeaderDecoder should handle already sent operations
            queue.drain(callback -> callback.cancel(address, cause));
            connectionFailureListener.accept(this, ChannelFactory.ChannelEventType.CONNECT_FAILED);
         } else {
            assert c.eventLoop().inEventLoop();
            connectionFailureListener.accept(this, ChannelFactory.ChannelEventType.CONNECTED);
            channel = c;
            headerDecoder = c.pipeline().get(HeaderDecoder.class);
            sendOperations();
         }
      });
   }

   @Override
   public void acquire(ChannelOperation callback) {
      queue.offer(callback);
      if (channel != null) {
         if (channel.eventLoop().inEventLoop()) {
            sendOperations();
         } else {
            channel.eventLoop().submit(this::sendOperations);
         }
      } else {
         attemptConnect();
      }
   }

   private void sendOperations() {
      if (channel == null || queue.isEmpty()) {
         return;
      }
      assert channel.eventLoop().inEventLoop();
      log.tracef("Commands enqueue to send to channel %s", channel);
      queue.drain(this);
      if (buffer != null) {
         log.tracef("Flushing commands to channel %s", channel);
         channel.writeAndFlush(buffer, channel.voidPromise());
         buffer = null;
      }
   }

   @Override
   public void release(Channel channel, ChannelRecord record) {
      // Do nothing
   }

   @Override
   public void releaseClosedChannel(Channel channel, ChannelRecord channelRecord) {
      assert channel.eventLoop().inEventLoop();
      assert channel == this.channel;
      attemptedConnect.set(false);
      this.channel = null;

      this.headerDecoder = null;

      connectionFailureListener.accept(this, ChannelFactory.ChannelEventType.CLOSED_ACTIVE);
   }

   @Override
   public SocketAddress getAddress() {
      return address;
   }

   @Override
   public int getActive() {
      return channel != null && !queue.isEmpty() ? 1 : 0;
   }

   @Override
   public int getIdle() {
      return channel != null && queue.isEmpty() ? 1 : 0;
   }

   @Override
   public int getConnected() {
      return getActive();
   }

   @Override
   public void close() {
      TransportException cause = new TransportException("Pool was closing", address);
      // HeaderDecoder should handle already sent operations
      queue.drain(callback -> {
         // Let HotRodOperations try to be retried
         if (callback instanceof HotRodOperation) {
            ((HotRodOperation<?>) callback).exceptionCaught(channel, cause);
         } else {
            callback.cancel(address, cause);
         }
      });
      if (channel != null) {
         // We don't want to fail all operations on given channel,
         // e.g. when moving from unresolved to resolved addresses
         channel.pipeline().fireUserEventTriggered(ChannelPoolCloseEvent.INSTANCE);
      }
   }

   @Override
   public void inspectPool() {
      if (channel == null) {
         attemptConnect();
      }
   }

   @Override
   public void accept(ChannelOperation channelOperation) {
      if (!(channelOperation instanceof HotRodOperation)) {
         assert channel != null;
         channelOperation.invoke(channel);
         return;
      }
      if (buffer == null) {
         buffer = channel.alloc().buffer();
      }
      HotRodOperation<?> operation = (HotRodOperation<?>) channelOperation;
      headerDecoder.registerOperation(channel, operation);

      operation.writeBytes(channel, buffer);
   }

   @Override
   public Queue<ChannelOperation> pendingChannelOperations() {
      return queue;
   }
}
