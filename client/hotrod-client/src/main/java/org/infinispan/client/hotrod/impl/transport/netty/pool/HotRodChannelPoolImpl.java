package org.infinispan.client.hotrod.impl.transport.netty.pool;

import static org.infinispan.client.hotrod.logging.Log.HOTROD;

import java.net.SocketAddress;
import java.util.Deque;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.configuration.ExhaustedAction;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelKeys;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelPoolCloseEvent;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.Promise;
import io.netty.util.concurrent.ScheduledFuture;
import io.netty.util.internal.PlatformDependent;

/**
 * This is a custom implementation of {@link io.netty.channel.Channel} pooling.
 * Compared to {@link io.netty.channel.pool.ChannelPool} implementations in Netty it does not enforce context switch before writing to the channel.
 * **Update**: Netty enforces going through event loop later on by delegating the write through io.netty.channel.AbstractChannelHandlerContext.WriteTask.
 * So writing the socket in caller thread is still TODO.
 * <p>
 * It should be also more allocation-efficient since it does not create futures and invokes the callback directly if the
 * channel is available.
 * <p>
 * The connections are handled LIFO, pending requests are handled FIFO.
 */
public class HotRodChannelPoolImpl implements HotRodChannelPool {

   private static final Log log = LogFactory.getLog(HotRodChannelPoolImpl.class);
   private static final int MAX_FULL_CHANNELS_SEEN = 10;

   private final Bootstrap bootstrap;
   private final HotRodChannelPoolHandler poolHandler;
   private final int maxConnections;
   private final int maxPendingRequests;
   private final ExhaustedAction exhaustedAction;
   private long maxWait;

   private final EventExecutor executor;
   private final Deque<Channel> channels;
   private final Deque<PendingRequest> pendingRequests;

   private SocketAddress unresolvedAddress;

   public HotRodChannelPoolImpl(Bootstrap bootstrap, HotRodChannelPoolHandler poolHandler, int maxConnections,
                                int maxPendingRequests, ExhaustedAction exhaustedAction, long maxWait,
                                SocketAddress unresolvedAddress) {
      this.bootstrap = bootstrap;
      this.poolHandler = poolHandler;
      this.maxConnections = maxConnections;
      this.maxPendingRequests = maxPendingRequests;
      this.exhaustedAction = exhaustedAction;
      this.maxWait = maxWait;

      this.executor = bootstrap.config().group().next();
      this.channels = PlatformDependent.newConcurrentDeque();
      this.pendingRequests = PlatformDependent.newConcurrentDeque();

      this.unresolvedAddress = unresolvedAddress;
   }

   private Channel pollChannel() {
      Channel channel;
      int fullChannelsSeen = 0;
      while ((channel = channels.pollFirst()) != null) {
         if (!channel.isActive()) {
            // The channel was closed while idle but not removed - just forget it
            continue;
         }
         if (!channel.isWritable() || channel.pipeline().get(HeaderDecoder.class).registeredOperations() >= maxPendingRequests) {
            channels.addLast(channel);
            // prevent looping on non-writable channels
            if (++fullChannelsSeen < MAX_FULL_CHANNELS_SEEN) {
               continue;
            } else {
               break;
            }
         }
         return channel;
      }
      return null;
   }

   private void drainPending() {
      synchronized (pendingRequests) {
         PendingRequest pendingRequest = pendingRequests.peekFirst();
         if (pendingRequest != null) {
            Channel fetchChannel = this.pollChannel();
            if (fetchChannel != null) {
               pendingRequest.setSuccess(fetchChannel);
            }
         }
      }
   }

   @Override
   public Future<Channel> acquire() {
      return acquire(bootstrap.config().group().next().<Channel>newPromise());
   }

   @Override
   public Future<Channel> acquire(final Promise<Channel> promise) {
      drainPending();
      promise.addListener((GenericFutureListener<Future<Channel>>) listener -> {
         Channel channelNow = listener.getNow();
         poolHandler.channelAcquired(channelNow);
         if (!listener.isSuccess()) {
            decreasePoolCounters(channelNow, listener.cause());
         }
      });
      Channel fetchChannel = this.pollChannel();
      if (fetchChannel != null) {
         promise.setSuccess(fetchChannel);
      } else if (ExhaustedAction.CREATE_NEW.equals(exhaustedAction) || this.calculateMaxConnections()) {
         createConnection(promise);
      } else if (exhaustedAction.equals(ExhaustedAction.EXCEPTION)) {
         throw new IllegalStateException("Reached maximum number of connections");
      } else if (exhaustedAction.equals(ExhaustedAction.WAIT)) {
         synchronized (pendingRequests) {
            PendingRequest task = new PendingRequest(promise);
            if (maxWait > 0) {
               ScheduledFuture<?> scheduledFuture = executor.schedule(task, maxWait, TimeUnit.MILLISECONDS);
               task.setScheduledFuture(scheduledFuture);
            }
            pendingRequests.addLast(task);
            drainPending();
         }
      } else {
         throw new IllegalArgumentException("Exhausted Action not implemented: " + exhaustedAction);
      }
      return promise;
   }

   private void createConnection(Promise<Channel> promise) {
      try {
         if (log.isTraceEnabled()) log.tracef("[%s] Creating new channel, created = %d, active = %d", this.unresolvedAddress, poolHandler.getChannelCreated(), poolHandler.getChannelAcquired());
         Bootstrap clonedBootstrap = bootstrap.clone();
         clonedBootstrap.handler(new ChannelInitializer<Channel>() {
            @Override
            protected void initChannel(Channel ch) throws Exception {
               assert ch.eventLoop().inEventLoop();
               poolHandler.channelCreated(ch);
            }
         });
         ChannelFuture channelFuture = clonedBootstrap.connect();
         channelFuture.addListener((ChannelFutureListener) future -> {
            if (future.isSuccess()) {
               Channel channel = future.channel();
               channel.closeFuture().addListener((GenericFutureListener<ChannelFuture>) closeFuture -> {
                  Channel futureChannel = closeFuture.channel();
                  if (log.isTraceEnabled()) {
                     if (!closeFuture.isSuccess()) {
                        log.tracef(closeFuture.cause(), "Channel %s is closed, see exception for details", channel);
                     }
                  }
                  releaseClosedChannel(futureChannel, ChannelKeys.getChannelRecord(futureChannel).isIdle());
               });
               ChannelKeys.getActivationListener(channel).whenComplete((futureChannel, throwable) -> {
                  if (throwable != null) {
                     promise.setFailure(throwable);
                  } else {
                     promise.setSuccess(futureChannel);
                  }
               });
            } else {
               promise.setFailure(future.cause());
            }
         });
      } catch (Throwable t) {
         promise.setFailure(t);
         this.decreasePoolCounters(null, t);
         throw t;
      }
   }

   private void decreasePoolCounters(Channel channel, Throwable t) {
      poolHandler.decreasePoolCounters();
      int currentActive = poolHandler.getChannelAcquired();
      int currentCreated = poolHandler.getChannelCreated();

      assert currentActive >= 0;
      assert currentCreated >= 0;

      if (log.isTraceEnabled()) log.tracef(t, "[%s] Channel could not be created, created = %d, active = %d",
            this.unresolvedAddress, currentCreated, currentActive);
      if (currentCreated < 0) {
         HOTROD.invalidActiveCountAfterClose(channel != null ? channel.toString() : "<channel not initialized>");
      }
      if (currentActive < 0) {
         HOTROD.invalidActiveCountAfterClose(channel != null ? channel.toString() : "<channel not initialized>");
      }
   }

   private boolean calculateMaxConnections() {
      return poolHandler.getChannelCreated() < maxConnections;
   }

   @Override
   public void close() {
      channels.forEach(channel -> {
         // We don't want to fail all operations on given channel,
         // e.g. when moving from unresolved to resolved addresses
         channel.pipeline().fireUserEventTriggered(ChannelPoolCloseEvent.INSTANCE);
      });
      synchronized (pendingRequests) {
         pendingRequests.forEach(p -> p.tryFailure(new IllegalStateException("Closed channel")));
      }
   }

   @Override
   public Future<Void> release(Channel channel) {
      poolHandler.channelReleased(channel);
      this.channels.addFirst(channel);
      drainPending();
      return null;
   }

   @Override
   public Future<Void> release(Channel channel, Promise<Void> promise) {
      throw new UnsupportedOperationException("Release with promise is not supported");
   }

   private void releaseClosedChannel(Channel channel, boolean idle) {
      if (channel.isActive()) {
         HOTROD.warnf("[%s] Channel %s cannot be released because it is not closed", ChannelKeys.getUnresolvedAddress(channel), channel);
      } else {
         poolHandler.decreaseChannelCreated();
         int currentCreated = poolHandler.getChannelCreated();
         if (!idle) {
            poolHandler.channelReleased(channel);
         }
         int currentActive = poolHandler.getChannelAcquired();

         if (log.isTraceEnabled()) log.tracef("[%s] Closed channel %s, created = %s, idle = %b, active = %d",
               ChannelKeys.getUnresolvedAddress(channel), channel, currentCreated, idle, currentActive);
         if (currentCreated < 0) {
            HOTROD.warnf("[%s] Invalid created count after closing channel %s", ChannelKeys.getUnresolvedAddress(channel), channel);
         }
         if (currentActive < 0) {
            HOTROD.warnf("[%s] Invalid active count after closing channel %s", ChannelKeys.getUnresolvedAddress(channel), channel);
         }
      }
   }

   @Override
   public int getCreated() {
      return poolHandler.getChannelCreated();
   }

   @Override
   public int getActive() {
      return poolHandler.getChannelAcquired();
   }

   @Override
   public int getIdle() {
      return Math.max(0, poolHandler.getChannelCreated() - poolHandler.getChannelAcquired());
   }

   private class PendingRequest implements Runnable {
      private final Promise<Channel> originalPromise;
      private ScheduledFuture<?> scheduledFuture;
      public PendingRequest(Promise<Channel> originalPromise) {
         this.originalPromise = originalPromise;
      }

      @Override
      public void run() {
         originalPromise.tryFailure(new IllegalStateException("Timeout Exception"));
         pendingRequests.remove(this);
      }

      public void setScheduledFuture(ScheduledFuture<?> scheduledFuture) {
         this.scheduledFuture = scheduledFuture;
      }

      public void setSuccess(Channel channel) {
         pendingRequests.remove(this);
         originalPromise.setSuccess(channel);
         if (scheduledFuture != null) {
            scheduledFuture.cancel(false);
         }
      }

      public void tryFailure(Throwable cause) {
         pendingRequests.remove(this);
         originalPromise.tryFailure(cause);
         if (scheduledFuture != null) {
            scheduledFuture.cancel(false);
         }
      }
   }

   @Override
   public String toString() {
      return "HotRodChannelPoolImpl[" +
            ", maxPendingRequests=" + maxPendingRequests +
            ", created=" + poolHandler.getChannelCreated() +
            ", active=" + poolHandler.getChannelAcquired() +
            ", maxConnections=" + maxConnections +
            ", maxWait=" + maxWait +
            ']';
   }
}
