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
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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
   private static final AtomicIntegerFieldUpdater<TimeoutCallback> invokedUpdater = AtomicIntegerFieldUpdater.newUpdater(TimeoutCallback.class, "invoked");
   private static final Log log = LogFactory.getLog(ChannelPool.class);
   private static final boolean trace = log.isTraceEnabled();
   private static final int MAX_FULL_CHANNELS_SEEN = 10;

   private final Deque<Channel> channels = PlatformDependent.newConcurrentDeque();
   private final Deque<ChannelOperation> callbacks = PlatformDependent.newConcurrentDeque();
   private final EventExecutor executor;
   private final SocketAddress address;
   private final ChannelInitializer newChannelInvoker;
   private final ExhaustedAction exhaustedAction;
   private final long maxWait;
   private final int maxConnections;
   private final int maxPendingRequests;
   private final AtomicInteger created = new AtomicInteger();
   private final AtomicInteger active = new AtomicInteger();
   private final ReadWriteLock lock = new ReentrantReadWriteLock();
   private volatile boolean terminated = false;

   ChannelPool(EventExecutor executor, SocketAddress address, ChannelInitializer newChannelInvoker, ExhaustedAction exhaustedAction, long maxWait, int maxConnections, int maxPendingRequests) {
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
            if (trace) log.tracef("Creating new channel, created = %d, active = %d", current + 1, currentActive);
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
            if (trace) log.tracef("Creating new channel, created = %d, active = %d", currentCreated, currentActive);
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
      lock.writeLock().lock();
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
         lock.writeLock().unlock();
      }
      activateChannel(channel, callback, false);
   }

   private void createAndInvoke(ChannelOperation callback) {
      try {
         newChannelInvoker.createChannel().whenComplete((channel, throwable) -> {
            if (throwable != null) {
               int currentActive = active.decrementAndGet();
               assert currentActive >= 0;
               int currentCreated = created.decrementAndGet();
               assert currentCreated >= 0;
               if (trace) log.tracef(throwable, "Channel could not be created, created = %d, active = %d",
                                     currentCreated, currentActive);
               callback.cancel(address, throwable);
            } else {
               callback.invoke(channel);
            }
         });
      } catch (Throwable t) {
         int currentActive = active.decrementAndGet();
         int currentCreated = created.decrementAndGet();
         if (trace) log.tracef(t, "Channel could not be created, created = %d, active = %d",
                               currentCreated, currentActive);
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
         if (trace) log.tracef("Attempt to release already closed channel %s, active = %d", channel, active.get());
         return;
      }

      int currentActive = active.decrementAndGet();
      if (trace) log.tracef("Released channel %s, active = %d", channel, currentActive);
      if (currentActive < 0) {
         HOTROD.warnf("Invalid active count after releasing channel %s", channel);
      }

      ChannelOperation callback;
      // We're protecting against concurrent acquires, concurrent releases are fine
      // hopefully the acquire will usually get the channel through the fast (non-locking) path
      lock.readLock().lock();
      try {
         callback = callbacks.pollFirst();
         if (callback == null) {
            channels.addFirst(channel);
            return;
         }
      } finally {
         lock.readLock().unlock();
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
      if (trace) log.tracef("Closed channel %s, created = %s, idle = %b, active = %d",
                            channel, currentCreated, idle, currentActive);
      if (currentCreated < 0) {
         HOTROD.warnf("Invalid created count after closing channel %s", channel);
      }
      if (currentActive < 0) {
         HOTROD.warnf("Invalid active count after closing channel %s", channel);
      }
   }

   private void activateChannel(Channel channel, ChannelOperation callback, boolean useExecutor) {
      assert channel.isActive() : "Channel " + channel + " is not active";
      int currentActive = active.incrementAndGet();
      if (trace) log.tracef("Activated record %s, created = %d, active = %d", channel, created.get(), currentActive);
      ChannelRecord record = ChannelRecord.of(channel);
      record.setAcquired();
      if (useExecutor) {
         // Do not execute another operation in releasing thread, we could run out of stack
         executor.execute(() -> {
            try {
               callback.invoke(channel);
            } catch (Throwable t) {
               log.tracef(t, "Closing channel %s due to exception", channel);
               discardChannel(channel, record);
            }
         });
      } else {
         try {
            callback.invoke(channel);
         } catch (Throwable t) {
            log.tracef(t, "Closing channel %s due to exception", channel);
            discardChannel(channel, record);
            throw t;
         }
      }
   }

   private void discardChannel(Channel channel, ChannelRecord record) {
      channel.close();
   }

   public int getActive() {
      return active.get();
   }

   public int getIdle() {
      return Math.max(0, created.get() - active.get());
   }

   public void close() {
      terminated = true;
      lock.writeLock().lock();
      try {
         RejectedExecutionException cause = new RejectedExecutionException("Pool was terminated");
         callbacks.forEach(callback -> callback.cancel(address, cause));
         channels.forEach(channel -> {
            // We don't want to fail all operations on given channel,
            // e.g. when moving from unresolved to resolved addresses
            channel.pipeline().fireUserEventTriggered(ChannelPoolCloseEvent.INSTANCE);
         });
      } finally {
         lock.writeLock().unlock();
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
