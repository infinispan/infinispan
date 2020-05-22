package org.infinispan.client.hotrod.impl.transport.netty;

import java.net.SocketAddress;
import java.util.concurrent.CompletableFuture;

import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.GenericFutureListener;

/**
 * This class serves multiple purposes:
 *
 * 1) Activation: this extends {@link CompletableFuture} which is completed once the connection and initial handshake
 * are completed.
 * 2) Storage for unresolved address and pool info.
 */
public class ChannelRecord extends CompletableFuture<Channel> implements GenericFutureListener<ChannelFuture> {
   private static final Log log = LogFactory.getLog(ChannelRecord.class);
   private static final boolean trace = log.isTraceEnabled();

   static AttributeKey<ChannelRecord> KEY = AttributeKey.newInstance("activation");

   private final SocketAddress unresolvedAddress;
   private final ChannelPool channelPool;
   private boolean closed = false;
   private boolean acquired = true;

   ChannelRecord(SocketAddress unresolvedAddress, ChannelPool channelPool) {
      this.unresolvedAddress = unresolvedAddress;
      this.channelPool = channelPool;
   }

   public static ChannelRecord of(Channel channel) {
      return channel.attr(KEY).get();
   }

   public SocketAddress getUnresolvedAddress() {
      return unresolvedAddress;
   }

   @Override
   public boolean complete(Channel channel) {
      channel.closeFuture().addListener(this);
      return super.complete(channel);
   }

   @Override
   public void operationComplete(ChannelFuture future) throws Exception {
      if (trace) {
         if (!future.isSuccess()) {
            log.tracef(future.cause(), "Channel %s is closed, see exception for details", get());
         }
      }
      channelPool.releaseClosedChannel(future.channel(), this);
   }

   synchronized void setAcquired() {
      assert !acquired;
      acquired = true;
   }

   public synchronized boolean isIdle() {
      return !acquired;
   }

   public synchronized boolean setIdleAndIsClosed() {
      assert acquired;
      acquired = false;

      return closed;
   }

   public synchronized boolean closeAndWasIdle() {
      assert !closed;
      closed = true;

      return !acquired;
   }

   public void release(Channel channel) {
      channelPool.release(channel, this);
   }
}
