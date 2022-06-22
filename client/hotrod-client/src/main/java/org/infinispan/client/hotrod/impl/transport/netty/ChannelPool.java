package org.infinispan.client.hotrod.impl.transport.netty;

import static org.infinispan.client.hotrod.logging.Log.HOTROD;

import java.net.SocketAddress;
import java.util.Deque;
import java.util.NoSuchElementException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.locks.StampedLock;
import java.util.function.BiConsumer;

import org.infinispan.client.hotrod.configuration.ExhaustedAction;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;

import io.netty.channel.Channel;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.ScheduledFuture;
import io.netty.util.internal.PlatformDependent;

/**
 * This is a custom implementation of {@link io.netty.channel.Channel} pooling.
 * Compared to {@link io.netty.channel.pool.ChannelPool} implementations in Netty it does not enforce context switch before writing to the channel.
 * **Update**: Netty enforces going through event loop later on by delegating the write through {@link io.netty.channel.AbstractChannelHandlerContext.WriteTask}.
 * So writing the socket in caller thread is still TODO.
 * <p>
 * It should be also more allocation-efficient since it does not create futures and invokes the callback directly if the
 * channel is available.
 * <p>
 * The connections are handled LIFO, pending requests are handled FIFO.
 */
class ChannelPool {
   enum ChannelEventType { CONNECTED, CLOSED_IDLE, CLOSED_ACTIVE, CONNECT_FAILED}
   private static final AtomicIntegerFieldUpdater<TimeoutCallback> invokedUpdater = AtomicIntegerFieldUpdater.newUpdater(TimeoutCallback.class, "invoked");
   private static final Log log = LogFactory.getLog(ChannelPool.class);
   private static final int MAX_FULL_CHANNELS_SEEN = 10;

   private final Deque<Channel> channels = PlatformDependent.newConcurrentDeque();
   private final Deque<ChannelOperation> callbacks = PlatformDependent.newConcurrentDeque();
   private final EventExecutor executor;
   private final SocketAddress address;
   private final ChannelInitializer newChannelInvoker;
   private final ExhaustedAction exhaustedAction;
   private final BiConsumer<ChannelPool, ChannelEventType> connectionFailureListener;
   private final long maxWait;
   private final int maxConnections;
   private final int maxPendingRequests;
   private final AtomicInteger created = new AtomicInteger();
   private final AtomicInteger active = new AtomicInteger();
   private final AtomicInteger connected = new AtomicInteger();
   private final StampedLock lock = new StampedLock();
   private volatile boolean terminated = false;

   ChannelPool(EventExecutor executor, SocketAddress address, ChannelInitializer newChannelInvoker,
               ExhaustedAction exhaustedAction, BiConsumer<ChannelPool, ChannelEventType> connectionFailureListener,
               long maxWait, int maxConnections, int maxPendingRequests) {
      this.connectionFailureListener = connectionFailureListener;
      this.executor = executor;
      this.address = address;
      this.newChannelInvoker = newChannelInvoker;
      this.exhaustedAction = exhaustedAction;
      this.maxWait = maxWait;
      this.maxConnections = maxConnections;
      this.maxPendingRequests = maxPendingRequests;
   }

   public void acquire(ChannelOperation callback) {
      if (terminated) {
         callback.cancel(address, new RejectedExecutionException("Pool was terminated"));
         return;
      }
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
         activateChannel(channel, callback, false);
         return;
      }
      int current = created.get();
      while (current < maxConnections) {
         if (created.compareAndSet(current, current + 1)) {
            int currentActive = active.incrementAndGet();
            if (log.isTraceEnabled()) log.tracef("[%s] Creating new channel, created = %d, active = %d", address, current + 1, currentActive);
            // create new connection and apply callback
            createAndInvoke(callback);
            return;
         }
         current = created.get();
      }
      // reached max connections
      switch (exhaustedAction) {
         case EXCEPTION:
            throw new NoSuchElementException("Reached maximum number of connections");
         case WAIT:
            break;
         case CREATE_NEW:
            int currentCreated = created.incrementAndGet();
            int currentActive = active.incrementAndGet();
            if (log.isTraceEnabled()) log.tracef("[%s] Creating new channel, created = %d, active = %d", address, currentCreated, currentActive);
            createAndInvoke(callback);
            return;
         default:
            throw new IllegalArgumentException(String.valueOf(exhaustedAction));
      }
      // wait action
      if (maxWait > 0) {
         TimeoutCallback timeoutCallback = new TimeoutCallback(callback);
         timeoutCallback.timeoutFuture = executor.schedule(timeoutCallback, maxWait, TimeUnit.MILLISECONDS);
         callback = timeoutCallback;
      }
      // To prevent adding channel and callback concurrently we'll synchronize all additions
      // TODO: completely lock-free algorithm would be better
      long stamp = lock.writeLock();
      try {
         for (;;) {
            // at this point we won't be picky and use non-writable channel anyway
            channel = channels.pollFirst();
            if (channel == null) {
               callbacks.addLast(callback);
               return;
            } else if (channel.isActive()) {
               break;
            }
         }
      } finally {
         lock.unlockWrite(stamp);
      }
      activateChannel(channel, callback, false);
   }

   private void createAndInvoke(ChannelOperation callback) {
      try {
         newChannelInvoker.createChannel().whenComplete((channel, throwable) -> {
            if (throwable != null) {
               int currentActive = active.decrementAndGet();
               if (currentActive < 0) {
                  HOTROD.invalidActiveCountAfterClose(channel);
               }
               int currentCreated = created.decrementAndGet();
               if (currentCreated < 0) {
                  HOTROD.invalidCreatedCountAfterClose(channel);
               }
               if (log.isTraceEnabled()) log.tracef(throwable, "[%s] Channel could not be created, created = %d, active = %d, connected = %d",
                                     address, currentCreated, currentActive, connected.get());
               callback.cancel(address, throwable);
               connectionFailureListener.accept(this, ChannelEventType.CONNECT_FAILED);
            } else {
               int currentConnected = connected.incrementAndGet();
               if (log.isTraceEnabled()) log.tracef("[%s] Channel connected, created = %d, active = %d, connected = %d",
                                                    address, created.get(), active.get(), currentConnected);
               callback.invoke(channel);
               connectionFailureListener.accept(this, ChannelEventType.CONNECTED);
            }
         });
      } catch (Throwable t) {
         int currentActive = active.decrementAndGet();
         int currentCreated = created.decrementAndGet();
         if (log.isTraceEnabled()) log.tracef(t, "[%s] Channel could not be created, created = %d, active = %d, connected = %d",
                               address, currentCreated, currentActive, connected.get());
         if (currentCreated < 0) {
            HOTROD.warnf("Invalid created count after channel create failure");
         }
         if (currentActive < 0) {
            HOTROD.warnf("Invalid active count after channel create failure");
         }
         callback.cancel(address, t);
      }
   }

   /**
    * Release a channel back into the pool after an operation has finished.
    */
   public void release(Channel channel, ChannelRecord record) {
      // The channel can be closed when it's idle (due to idle timeout or closed connection)
      if (record.isIdle()) {
         HOTROD.warnf("Cannot release channel %s because it is idle", channel);
         return;
      }

      if (record.setIdleAndIsClosed()) {
         if (log.isTraceEnabled()) log.tracef("[%s] Attempt to release already closed channel %s, active = %d",
                                              address, channel, active.get());
         return;
      }

      int currentActive = active.decrementAndGet();
      if (log.isTraceEnabled()) log.tracef("[%s] Released channel %s, active = %d", address, channel, currentActive);
      if (currentActive < 0) {
         HOTROD.warnf("[%s] Invalid active count after releasing channel %s", address, channel);
      }

      ChannelOperation callback;
      // We're protecting against concurrent acquires, concurrent releases are fine
      // hopefully the acquire will usually get the channel through the fast (non-locking) path
      long stamp = lock.readLock();
      try {
         callback = callbacks.pollFirst();
         if (callback == null) {
            channels.addFirst(channel);
            return;
         }
      } finally {
         lock.unlockRead(stamp);
      }
      activateChannel(channel, callback, true);
   }

   /**
    * Update counts after a channel has been closed.
    */
   public void releaseClosedChannel(Channel channel, ChannelRecord channelRecord) {
      if (channel.isActive()) {
         HOTROD.warnf("Channel %s cannot be released because it is not closed", channel);
         return;
      }

      boolean idle = channelRecord.closeAndWasIdle();

      int currentCreated = created.decrementAndGet();
      int currentActive = !idle ? active.decrementAndGet() : active.get();
      int currentConnected = connected.decrementAndGet();
      if (log.isTraceEnabled()) log.tracef("[%s] Closed channel %s, created = %s, idle = %b, active = %d, connected = %d",
                                           address, channel, currentCreated, idle, currentActive, currentConnected);
      if (currentCreated < 0) {
         HOTROD.warnf("Invalid created count after closing channel %s", channel);
      }
      if (currentActive < 0) {
         HOTROD.warnf("Invalid active count after closing channel %s", channel);
      }
      connectionFailureListener.accept( this, idle ? ChannelEventType.CLOSED_IDLE : ChannelEventType.CLOSED_ACTIVE);
   }

   private void activateChannel(Channel channel, ChannelOperation callback, boolean useExecutor) {
      assert channel.isActive() : "Channel " + channel + " is not active";
      int currentActive = active.incrementAndGet();
      if (log.isTraceEnabled()) log.tracef("[%s] Activated record %s, created = %d, active = %d", address, channel, created.get(), currentActive);
      ChannelRecord record = ChannelRecord.of(channel);
      record.setAcquired();
      if (useExecutor) {
         // Do not execute another operation in releasing thread, we could run out of stack
         executor.execute(() -> {
            try {
               callback.invoke(channel);
            } catch (Throwable t) {
               log.tracef(t, "Closing channel %s due to exception", channel);
               discardChannel(channel);
            }
         });
      } else {
         try {
            callback.invoke(channel);
         } catch (Throwable t) {
            log.tracef(t, "Closing channel %s due to exception", channel);
            discardChannel(channel);
            throw t;
         }
      }
   }

   private void discardChannel(Channel channel) {
      channel.close();
   }

   public SocketAddress getAddress() {
      return address;
   }

   public int getActive() {
      return active.get();
   }

   public int getIdle() {
      return Math.max(0, created.get() - active.get());
   }

   public int getConnected() {
      return connected.get();
   }

   public void close() {
      terminated = true;
      long stamp = lock.writeLock();
      try {
         RejectedExecutionException cause = new RejectedExecutionException("Pool was terminated");
         callbacks.forEach(callback -> callback.cancel(address, cause));
         channels.forEach(channel -> {
            // We don't want to fail all operations on given channel,
            // e.g. when moving from unresolved to resolved addresses
            channel.pipeline().fireUserEventTriggered(ChannelPoolCloseEvent.INSTANCE);
         });
      } finally {
         lock.unlockWrite(stamp);
      }
   }

   @Override
   public String toString() {
      return "ChannelPool[" +
            "address=" + address +
            ", maxWait=" + maxWait +
            ", maxConnections=" + maxConnections +
            ", maxPendingRequests=" + maxPendingRequests +
            ", created=" + created +
            ", active=" + active +
            ", connected=" + connected +
            ", terminated=" + terminated +
            ']';
   }

   private class TimeoutCallback implements ChannelOperation, Runnable {
      final ChannelOperation callback;
      volatile ScheduledFuture<?> timeoutFuture;
      @SuppressWarnings("unused")
      volatile int invoked = 0;

      private TimeoutCallback(ChannelOperation callback) {
         this.callback = callback;
      }

      @Override
      public void run() {
         callbacks.remove(this);
         if (invokedUpdater.compareAndSet(this, 0, 1)) {
            callback.cancel(address, new TimeoutException("Timed out waiting for connection"));
         }
      }

      @Override
      public void invoke(Channel channel) {
         ScheduledFuture<?> timeoutFuture = this.timeoutFuture;
         if (timeoutFuture != null) {
            timeoutFuture.cancel(false);
         }
         if (invokedUpdater.compareAndSet(this, 0, 1)) {
            callback.invoke(channel);
         }
      }

      @Override
      public void cancel(SocketAddress address, Throwable cause) {
         throw new UnsupportedOperationException();
      }
   }
}
