package org.infinispan.client.hotrod.impl.transport.netty;

import java.net.SocketAddress;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.BiConsumer;

import org.infinispan.client.hotrod.configuration.ExhaustedAction;
import org.infinispan.client.hotrod.impl.transport.netty.pool.HotRodChannelPool;
import org.infinispan.client.hotrod.impl.transport.netty.pool.HotRodChannelPoolHandler;
import org.infinispan.client.hotrod.impl.transport.netty.pool.HotRodChannelPoolImpl;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

/**
 * Retrieve the channel and execute the operation.
 */
class ChannelOperationHandler {
   enum ChannelEventType { CONNECTED, CLOSED_IDLE, CLOSED_ACTIVE, CONNECT_FAILED}

   private final SocketAddress unresolvedAddress;
   private final BiConsumer<ChannelOperationHandler, ChannelEventType> connectionFailureListener;
   private final HotRodChannelPool pool;
   private volatile boolean terminated = false;

   ChannelOperationHandler(Bootstrap bootstrap, HotRodChannelPoolHandler poolHandler, SocketAddress unresolvedAddress,
                           ExhaustedAction exhaustedAction, BiConsumer<ChannelOperationHandler, ChannelEventType> connectionFailureListener,
                           long maxWait, int maxConnections, int maxPendingRequests) {
      this.unresolvedAddress = unresolvedAddress;
      this.connectionFailureListener = connectionFailureListener;
      this.pool = new HotRodChannelPoolImpl(bootstrap, poolHandler, maxConnections, maxPendingRequests, exhaustedAction, maxWait, unresolvedAddress);
   }

   public SocketAddress getUnresolvedAddress() {
      return unresolvedAddress;
   }

   public void acquire(ChannelOperation callback) {
      try {
         if (terminated) {
            cancelCallback(callback, new RejectedExecutionException("Pool was terminated"));
         } else {
            Future<Channel> futureChannel = this.pool.acquire();
            activateChannel(futureChannel, callback);
         }
      } catch (Throwable cause) {
         cancelCallback(callback, cause);
      }
   }

   /**
    * It is a async chain method:
    *
    * Release a channel back into the pool after an operation has finished.
    * Execute a new operation async if there is one in the queue
    */
   public void release(Channel channel) {
      this.pool.release(channel);
   }

   private void activateChannel(Future<Channel> futureChannel, ChannelOperation callback) {
      futureChannel.addListener((GenericFutureListener<Future<Channel>>) listener -> {
         try {
            if (listener.isSuccess()) {
               Channel channel = listener.getNow();
               callback.invoke(channel);
               connectionFailureListener.accept(this, ChannelEventType.CONNECTED);
            } else {
               cancelCallback(callback, listener.cause());
            }
         } catch (Exception e) {
            cancelCallback(callback, e);
         }
      });
   }

   private void cancelCallback(ChannelOperation callback, Throwable e) {
      callback.cancel(unresolvedAddress, e);
      connectionFailureListener.accept(this, ChannelEventType.CONNECT_FAILED);
   }

   public int getActive() {
      return this.pool.getActive();
   }

   public int getIdle() {
      return this.pool.getIdle();
   }

   public void close() {
      terminated = true;
      this.pool.close();
   }

   @Override
   public String toString() {
      return "ChannelOperationHandler[" +
            "address=" + unresolvedAddress +
            ", terminated=" + terminated +
            ", pool=" + this.pool +
            ']';
   }
}
