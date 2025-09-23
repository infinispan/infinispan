package org.infinispan.client.hotrod.impl.transport.netty;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.infinispan.client.hotrod.ProtocolVersion;
import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.client.hotrod.impl.ClientTopology;
import org.infinispan.client.hotrod.impl.operations.HotRodOperation;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.AttributeKey;
import io.netty.util.internal.shaded.org.jctools.queues.MessagePassingQueue;
import io.netty.util.internal.shaded.org.jctools.queues.MpscUnboundedArrayQueue;

public class OperationChannel implements MessagePassingQueue.Consumer<HotRodOperation<?>> {
   private static final Log log = LogFactory.getLog(OperationChannel.class);

   public static final AttributeKey<OperationChannel> OPERATION_CHANNEL_ATTRIBUTE_KEY = AttributeKey.newInstance("hotrod-operation");

   private final Runnable SEND_OPERATIONS = this::sendOperations;
   private final SocketAddress address;
   private final ChannelInitializer newChannelInvoker;
   private final AtomicReference<CompletableFuture<Void>> attemptedConnect = new AtomicReference<>();

   private final Function<String, ClientTopology> currentCacheTopologyFunction;
   private final BiConsumer<OperationChannel, Throwable> connectionFailureListener;
   // Unfortunately MessagePassingQueue doesn't implement Queue so we use the concrete class
   private final MpscUnboundedArrayQueue<HotRodOperation<?>> queue = new MpscUnboundedArrayQueue<>(128);

   // Volatile as operations can be submitted outside of the event loop (channel only written in event loop once)
   private volatile Channel channel;

   // All variable after this can ONLY be read/written while in the event loop

   // Here to signal that non authenticated operations can now proceed
   private boolean acceptingRequests;
   // Will be initialized to configured default, but it is possible it is overridden during the negotiation process
   Codec codec;
   HeaderDecoder headerDecoder;
   ByteBuf buffer;

   OperationChannel(SocketAddress unresolvedAddress, ChannelInitializer channelInitializer,
                    Function<String, ClientTopology> currentCacheTopologyFunction, BiConsumer<OperationChannel, Throwable> connectionFailureListener) {
      assert !(unresolvedAddress instanceof InetSocketAddress) || ((InetSocketAddress) unresolvedAddress).isUnresolved();
      this.address = unresolvedAddress;
      this.newChannelInvoker = channelInitializer;
      this.currentCacheTopologyFunction = currentCacheTopologyFunction;
      this.connectionFailureListener = connectionFailureListener;
   }

   public static OperationChannel createAndStart(SocketAddress address, ChannelInitializer newChannelInvoker,
                                                  Function<String, ClientTopology> currentCacheTopologyFunction,
                                                  BiConsumer<OperationChannel, Throwable> connectionFailureListener) {
      OperationChannel operationChannel = new OperationChannel(address, newChannelInvoker, currentCacheTopologyFunction, connectionFailureListener);
      operationChannel.attemptConnect();
      return operationChannel;
   }

   CompletionStage<Void> attemptConnect() {
      CompletableFuture<Void> connectFuture = new CompletableFuture<>();
      {
         CompletableFuture<Void> prev;
         if ((prev = attemptedConnect.compareAndExchange(null, connectFuture)) != null) {
            return prev;
         }
      }
      channel = null;
      ChannelFuture channelFuture = newChannelInvoker.createChannel();
      // This check here is in case if there is an issue with the event loop executor where it doesn't actually run
      // the listener above, which can happen if the event loop group is shutdown
      Throwable immediateError = channelFuture.cause();
      if (immediateError == null) {
         // It is possible for channelActive calls and this to be done in non deterministic ordering
         channelFuture.addListener(f -> {
            if (f.isSuccess()) {
               Channel c = channelFuture.channel();
               assert c.eventLoop().inEventLoop();
               c.attr(OPERATION_CHANNEL_ATTRIBUTE_KEY).set(this);
               channel = c;
               headerDecoder = c.pipeline().get(HeaderDecoder.class);
               codec = headerDecoder.getConfiguration().version().getCodec();
               connectionFailureListener.accept(this, null);
               // Now we can let auth or operations to continue
               channel.pipeline().fireUserEventTriggered(ActivationHandler.ACTIVATION_EVENT);
               log.tracef("OperationChannel %s connect complete to %s", this, c);
            } else {
               Throwable cause = f.cause();
               log.tracef("Connection attempt to %s encountered exception for %s", address, cause);
               // Allow another attempt later
               handleError(connectFuture, cause);
            }
         });
         // Just in case if the future had an exception while adding the listener
         immediateError = channelFuture.cause();
      }
      if (immediateError != null) {
         Throwable innerError = immediateError;
         log.tracef("Connection to %s encountered immediate exception from %s", address, innerError);
         // Allow another attempt later - submit this in event loop as we can't run while holding write lock
         channel.eventLoop().execute(() -> handleError(connectFuture, innerError));
      }
      return connectFuture;
   }

   private void handleError(CompletableFuture<Void> connectFuture, Throwable throwable) {
      // Allow another attempt later
      connectFuture.completeExceptionally(throwable);
      attemptedConnect.compareAndSet(connectFuture, null);
      TransportException transportCause = new TransportException(throwable, address);
      // HeaderDecoder should handle already sent operations
      connectionFailureListener.accept(this, transportCause);
   }

   public void setCodec(Codec codec) {
      assert channel.eventLoop().inEventLoop();
      this.codec = codec;
   }

   public boolean isAcceptingRequests() {
      var f = attemptedConnect.get();
      return f != null && f.isDone();
   }

   public void markAcceptingRequests() {
      channel.eventLoop().submit(() -> {
         // We can't mark it as complete until we are authenticated
         attemptedConnect.get().complete(null);
         acceptingRequests = true;
         sendOperations();
      });
   }

   /**
    * Allows sending a command directly without enqueuing. Note that this method should only be invoked after
    * the channel has been started and notified via the {@link ActivationHandler#ACTIVATION_EVENT} and after all netty
    * channel handlers have been {@link io.netty.channel.ChannelInboundHandler#channelActive(ChannelHandlerContext)}.
    * @param operation operation to send immediately, usually for login purposes
    */
   public void forceSendOperation(HotRodOperation<?> operation) {
      if (!channel.eventLoop().inEventLoop()) {
         throw new IllegalArgumentException("Force sent operation " + operation + " are required to be sent in the event loop only " + channel.eventLoop());
      }
      log.tracef("Immediately sending operation %s to channel %s", operation, channel);
      long messageId = headerDecoder.registerOperation(operation);
      Channel channel = this.channel;
      ByteBuf buffer = channel.alloc().buffer();

      // The commands that are performed while unauthenticated are done without a server verified codec,
      // so we must use a codec version that is safe for handshake to determine which codec to use
      Codec codecToUse = codec.isUnsafeForTheHandshake() ? ProtocolVersion.SAFE_HANDSHAKE_PROTOCOL_VERSION.getCodec() : codec;

      codecToUse.writeHeader(buffer, messageId, currentCacheTopologyFunction.apply(operation.getCacheName()), operation);
      operation.writeOperationRequest(channel, buffer, codecToUse);
      channel.writeAndFlush(buffer, channel.voidPromise());
   }

   public void sendOperation(HotRodOperation<?> operation) {
      queue.offer(operation);
      Channel channel = this.channel;
      if (channel != null) {
         log.tracef("Enqueued operation %s to send to channel %s", operation, channel);
         // TODO: maybe implement a way to only submit if required
         channel.eventLoop().execute(SEND_OPERATIONS);
      } else {
         log.tracef("Enqueued operation %s to send to address %s when connected", operation, address);
         attemptConnect();
      }
   }

   public Iterable<HotRodOperation<?>> reconnect(Throwable t) {
      assert channel == null || channel.eventLoop().inEventLoop();
      Channel channel = this.channel;
      this.channel = null;
      CompletableFuture<Void> future = attemptedConnect.getAndSet(null);
      if (future != null) {
         future.completeExceptionally(t);
      }
      // If the channel was marked as accepting requests than we can reuse it
      if (acceptingRequests) {
         log.tracef("Attempting to reconnect channel %s after exception %s", channel, t);
         acceptingRequests = false;
         // Let us try to reconnect
         attemptConnect();
      } else {
         log.tracef("Channel %s was never fully accepted, not reconnecting after exception %s", channel, t);
      }
      List<HotRodOperation<?>> channelOps = new ArrayList<>();
      queue.drain(channelOps::add, Integer.MAX_VALUE);
      return channelOps;
   }

   private void sendOperations() {
      Channel channel = this.channel;
      assert channel == null || channel.eventLoop().inEventLoop();
      if (!acceptingRequests || queue.isEmpty() || channel == null) {
         return;
      }

      if (log.isTraceEnabled()) {
         log.tracef("OperationChannel %s Sending commands: %s enqueue to send to channel %s", this, queue.size(), channel);
      }

      // We only send up to 256 commands a time
      queue.drain(this, 256);
      if (buffer != null && buffer.isReadable()) {
         log.tracef("Flushing commands to channel %s", channel);
         channel.writeAndFlush(buffer, channel.voidPromise());
         buffer = null;
      }
      if (log.isTraceEnabled()) {
         log.tracef("Queue size after: %s", queue.size());
      }
      // If Queue wasn't empty try to send again, but note this is sent on eventLoop so other operations can
      // barge in between our calls
      if (!queue.isEmpty()) {
         log.tracef("Resubmitting as more operations in queue after sending");
         channel.eventLoop().execute(SEND_OPERATIONS);
      }
   }

   public SocketAddress getAddress() {
      return address;
   }

   @Override
   public void accept(HotRodOperation<?> operation) {
      try {
         if (buffer == null) {
            buffer = channel.alloc().buffer();
         }
         long messageId = headerDecoder.registerOperation(operation);
         codec.writeHeader(buffer, messageId, currentCacheTopologyFunction.apply(operation.getCacheName()), operation);
         operation.writeOperationRequest(channel, buffer, codec);
      } catch (Throwable t) {
         log.tracef(t, "Encountered exception while attempting to write to channel %s", channel);
      }
   }

   public Queue<HotRodOperation<?>> pendingChannelOperations() {
      return queue;
   }

   public List<HotRodOperation<?>> close() {
      acceptingRequests = false;
      CompletableFuture<Void> future = attemptedConnect.getAndSet(null);
      if (future != null) {
         // If we closed a channel before we even established the connection fully just finish normally.
         // This is most likely due to interruption or connecting to a server that returned a topology without
         // itself in the list (which can happen with DNS hostnames, proxy hosts, etc.).
         future.complete(null);
      }
      if (channel != null) {
         channel.close();
      }
      List<HotRodOperation<?>> channelOps = new ArrayList<>();
      queue.drain(channelOps::add, Integer.MAX_VALUE);
      return channelOps;
   }

   public Channel getChannel() {
      return channel;
   }

   @Override
   public String toString() {
      return "OperationChannel{" +
            "address=" + address +
            ", queue.size=" + queue.size() +
            ", channel=" + channel +
            ", acceptingRequests=" + acceptingRequests +
            '}';
   }
}
