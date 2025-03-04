package org.infinispan.client.hotrod.impl.transport.netty;

import static org.infinispan.client.hotrod.logging.Log.HOTROD;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Deque;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
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
import org.infinispan.client.hotrod.metrics.RemoteCacheManagerMetricsRegistry;

import io.netty.channel.Channel;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.ScheduledFuture;
import io.netty.util.internal.PlatformDependent;
import net.jcip.annotations.GuardedBy;

/**
 * This is a custom implementation of {@link io.netty.channel.Channel} pooling.
 * Compared to {@link io.netty.channel.pool.ChannelPool} implementations in Netty it does not enforce context switch before writing to the channel.
 * **Update**: Netty enforces going through event loop later on by delegating the write through {@link io.netty.channel.AbstractChannelHandlerContext.WriteTask}.
 * So writing the socket in caller thread is still TODO.
 * <p>
 * It should be also more allocation-efficient since it does not create futures and invokes the callback directly if the
 * channel is available.
 *
 * <h2>Thread-safety</h2>
 * <p>
 * There are two queues for managing connections and pending requests. The connections are handled LIFO, pending
 * requests are handled FIFO. Although each queue is thread-safe on it's own, operations over the two queues need to be
 * synchronized using a {@link StampedLock}.
 * </p>
 * <p>
 * The synchronization is necessary to make sure enqueued operations are visible when releasing a channel, and that
 * an idle channel is visible before enqueuing an operation. The idea followed is that:
 *
 * <ul>
 *    <li>Adding an operation uses a write lock;</li>
 *    <li>Existence checks uses a write lock;</li>
 *    <li>Polling an operation uses a read lock.</li>
 * </ul>
 *
 * Since the queues are thread-safe, the poll operation are visible even with the shared lock.
 * </p>
 */
class ChannelPool {
   enum ChannelEventType { CONNECTED, CLOSED_IDLE, CLOSED_ACTIVE, CONNECT_FAILED}
   private static final AtomicIntegerFieldUpdater<TimeoutCallback> invokedUpdater = AtomicIntegerFieldUpdater.newUpdater(TimeoutCallback.class, "invoked");
   private static final Log log = LogFactory.getLog(ChannelPool.class);
   private static final int MAX_FULL_CHANNELS_SEEN = 10;

   @GuardedBy("lock")
   private final Deque<Channel> channels = PlatformDependent.newConcurrentDeque();

   @GuardedBy("lock")
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
   private final Set<Object> metricsIds = ConcurrentHashMap.newKeySet();
   private final RemoteCacheManagerMetricsRegistry metricRegistry;
   private volatile boolean terminated = false;
   private volatile boolean suspected = false;

   ChannelPool(EventExecutor executor, SocketAddress address, ChannelInitializer newChannelInvoker,
               ExhaustedAction exhaustedAction, BiConsumer<ChannelPool, ChannelEventType> connectionFailureListener,
               long maxWait, int maxConnections, int maxPendingRequests, RemoteCacheManagerMetricsRegistry metricRegistry) {
      this.connectionFailureListener = connectionFailureListener;
      this.executor = executor;
      this.address = address;
      this.newChannelInvoker = newChannelInvoker;
      this.exhaustedAction = exhaustedAction;
      this.maxWait = maxWait;
      this.maxConnections = maxConnections;
      this.maxPendingRequests = maxPendingRequests;
      this.metricRegistry = metricRegistry;
      Map<String, String> tags = address instanceof InetSocketAddress isa ?
            Map.of("server", String.format("%s:%s", isa.getHostString(), isa.getPort())) :
            Map.of("server", UUID.randomUUID().toString());
      metricRegistry.createGauge("connection.pool.active", "The number of current active connections", this::getActive, tags, metricsIds::add);
      metricRegistry.createGauge("connection.pool.idle", "The number of idle connections", this::getIdle, tags, metricsIds::add);
      metricRegistry.createGauge("connection.pool.connected", "The number of connected connections", this::getConnected, tags, metricsIds::add);
   }

   public void acquire(ChannelOperation callback) {
      if (terminated) {
         callback.cancel(address, new RejectedExecutionException("Pool was terminated"));
         return;
      }

      // We could acquire an active channel and submit the callback.
      if (executeDirectlyIfPossible(callback, false)) return;

      // wait action
      if (maxWait > 0) {
         TimeoutCallback timeoutCallback = new TimeoutCallback(callback);
         timeoutCallback.timeoutFuture = executor.schedule(timeoutCallback, maxWait, TimeUnit.MILLISECONDS);
         callback = timeoutCallback;
      }

      // Between the check time and adding the callback to the queue, we could have a channel available.
      // Let's just try again, this execution bypasses the FIFO ordering.
      if (!executeOrEnqueue(callback)) {
         // If the operation executes, it will remove the operation just before executing.
         executeDirectlyIfPossible(callback, true);
      }

      // The pool was terminated while the callback tried to acquire a channel. Let's complete it with an exception.
      if (terminated) close();
   }

   boolean executeDirectlyIfPossible(ChannelOperation callback, boolean checkCallback) {
      Channel channel;
      int fullChannelsSeen = 0;
      while ((channel = channels.pollFirst()) != null) {
         if (!channel.isActive()) {
            // The channel was closed while idle but not removed - just forget it
            continue;
         }
         int capacity = 0;
         if (!channel.isWritable() || (capacity = retrieveDecoderUtilization(channel)) >= maxPendingRequests || capacity < 0) {
            // The HeaderDecoder was removed, the channel is in a shutdown process. We can forget it.
            if (capacity < 0) continue;

            channels.addLast(channel);
            // prevent looping on non-writable channels
            if (++fullChannelsSeen < MAX_FULL_CHANNELS_SEEN) {
               continue;
            } else {
               break;
            }
         }
         return activateChannel(channel, callback, false, checkCallback);
      }
      int current = created.get();
      while (current < maxConnections) {
         if (created.compareAndSet(current, current + 1)) {
            int currentActive = active.incrementAndGet();
            if (log.isTraceEnabled()) log.tracef("[%s] Creating new channel, created = %d, active = %d", address, current + 1, currentActive);
            // create new connection and apply callback
            createAndInvoke(callback, checkCallback);
            return true;
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
            createAndInvoke(callback, checkCallback);
            return true;
         default:
            throw new IllegalArgumentException(String.valueOf(exhaustedAction));
      }

      return false;
   }

   private int retrieveDecoderUtilization(Channel channel) {
      HeaderDecoder decoder = channel.pipeline().get(HeaderDecoder.class);
      return decoder == null
            ? -1
            : decoder.registeredOperations();
   }

   private boolean executeOrEnqueue(ChannelOperation callback) {
      Channel channel;
      // To prevent adding channel and callback concurrently we'll synchronize all additions
      // TODO: completely lock-free algorithm would be better
      long stamp = lock.writeLock();
      try {
         for (;;) {
            // at this point we won't be picky and use non-writable channel anyway
            channel = channels.pollFirst();
            if (channel == null) {
               if (log.isTraceEnabled()) log.tracef("[%s] No channel available, adding callback to the queue %s", address, callback);
               callbacks.addLast(callback);
               return false;
            } else if (channel.isActive()) {
               break;
            }
         }
      } finally {
         lock.unlockWrite(stamp);
      }
      return activateChannel(channel, callback, false, false);
   }

   private void createAndInvoke(ChannelOperation callback, boolean checkCallback) {
      try {
         newChannelInvoker.createChannel().whenComplete((channel, t) -> handleChannelCreated(callback, checkCallback, channel, t));
      } catch (Throwable t) {
         handleChannelCreated(callback, checkCallback, null, t);
      }
   }

   private void handleChannelCreated(ChannelOperation callback, boolean checkCallback, Channel channel, Throwable throwable) {
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
         // Update about a possibly failing server before cancelling the callback.
         connectionFailureListener.accept(this, ChannelEventType.CONNECT_FAILED);
         callback.cancel(address, throwable);
         maybeRejectPendingCallbacks(throwable);
      } else {
         suspected = false;
         int currentConnected = connected.incrementAndGet();
         if (log.isTraceEnabled()) log.tracef("[%s] Channel connected, created = %d, active = %d, connected = %d",
               address, created.get(), active.get(), currentConnected);
         invokeCallback(channel, callback, checkCallback);
         connectionFailureListener.accept(this, ChannelEventType.CONNECTED);
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

      if (terminated) {
         if (log.isTraceEnabled()) log.tracef("[%s] Attempt to release channel %s after termination, active = %d",
                                              address, channel, active.get());
         channel.pipeline().fireUserEventTriggered(ChannelPoolCloseEvent.INSTANCE);
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

      // Utilize the executor to avoid a stack overflow with multiple releases.
      activateChannel(channel, callback, true, false);
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

   private boolean activateChannel(Channel channel, ChannelOperation callback, boolean useExecutor, boolean checkCallback) {
      if (!channel.isActive()) return false;
      int currentActive = active.incrementAndGet();
      if (log.isTraceEnabled()) log.tracef("[%s] Activated record %s, created = %d, active = %d", address, channel, created.get(), currentActive);
      ChannelRecord record = ChannelRecord.of(channel);
      record.setAcquired();
      if (useExecutor) {
         // Do not execute another operation in releasing thread, we could run out of stack
         executor.execute(() -> invokeCallback(channel, callback, checkCallback));
      } else {
         return invokeCallback(channel, callback, checkCallback);
      }
      return true;
   }

   /**
    * Invokes the callback utilizing the given channel.
    * <p>
    * This method is central to avoid concurrently executing an enqueued operation. The {@param removeBeforeInvoke},
    * if set, ensures we only invoke the operation if it is still enqueued. This is the slow-path, where locks need
    * to be acquired.
    * </p>
    *
    * <p>
    * The channel is released back to the pool in case the operation is not in the queue.
    * </p>
    *
    * @param channel: The channel to invoke the callback on.
    * @param callback: The callback to invoke.
    * @param removeBeforeInvoke: Whether to de-queue the callback before executing it.
    * @return <code>true</code> if executed the callback, <code>false</code>, otherwise.
    */
   boolean invokeCallback(Channel channel, ChannelOperation callback, boolean removeBeforeInvoke) {
      if (removeBeforeInvoke && !removeCallback(callback)) {
         log.debugf("Operation %s picked-up twice, returning channel to pool", callback);
         release(channel, ChannelRecord.of(channel));
         return false;
      }

      try {
         callback.invoke(channel);
      } catch (Throwable t) {
         log.tracef(t, "Closing channel %s due to exception", channel);
         discardChannel(channel);
         throw t;
      }

      return true;
   }

   /**
    * Acquires an exclusive lock over the callbacks to remove the operation.
    * <p>
    * This method should be invoked just before the operation is executed. Make sure to check on the return
    * value in case of executing enqueued operations.
    * </p>
    *
    * @param operation: Operation to remove from the list.
    * @return <code>true</code> if the operation was still enqueued, <code>false</code>, otherwise.
    */
   private boolean removeCallback(ChannelOperation operation) {
      long stamp = lock.writeLock();
      try {
         return callbacks.remove(operation);
      } finally {
         lock.unlockWrite(stamp);
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
         callbacks.clear();
         channels.forEach(channel -> {
            // We don't want to fail all operations on given channel,
            // e.g. when moving from unresolved to resolved addresses
            channel.pipeline().fireUserEventTriggered(ChannelPoolCloseEvent.INSTANCE);
         });
      } finally {
         lock.unlockWrite(stamp);
      }
      metricsIds.forEach(metricRegistry::removeMetric);
      metricsIds.clear();
   }

   public void inspectPool() {
      if (terminated || suspected || getConnected() > 0 || getActive() > 0) return;

      ChannelOperation cb = acquireHead();
      if (cb != null) {
         int currentCreated = created.incrementAndGet();
         int currentActive = active.incrementAndGet();
         if (log.isTraceEnabled()) log.tracef("[%s] Creating new channel to inspect server, created = %d, active = %d", address, currentCreated, currentActive);
         suspected = true;
         createAndInvoke(cb, false);
      }
   }

   private void maybeRejectPendingCallbacks(Throwable t) {
      if (terminated || !suspected || getConnected() > 0 || getActive() > 0) return;

      ChannelOperation cb;
      while ((cb = acquireHead()) != null) {
         cb.cancel(address, t);
      }
   }

   private ChannelOperation acquireHead() {
      long stamp = lock.readLock();
      try {
         return callbacks.pollFirst();
      } finally {
         lock.unlockRead(stamp);
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
            ", suspected=" + suspected +
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
