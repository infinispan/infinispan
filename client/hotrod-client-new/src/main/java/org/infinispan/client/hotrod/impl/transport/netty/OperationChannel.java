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

import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.client.hotrod.impl.ClientTopology;
import org.infinispan.client.hotrod.impl.operations.HotRodOperation;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
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
            connectFuture.completeExceptionally(cause);
            attemptedConnect.compareAndSet(connectFuture, null);
            TransportException transportCause = new TransportException(cause, address);
            // HeaderDecoder should handle already sent operations
            connectionFailureListener.accept(this, transportCause);
         }
      });
      return connectFuture;
   }

   public void setCodec(Codec codec) {
      assert channel.eventLoop().inEventLoop();
      this.codec = codec;
   }

   public void markAcceptingRequests() {
      channel.eventLoop().submit(() -> {
         // We can't mark it as complete until we are authenticated
         attemptedConnect.get().complete(null);
         acceptingRequests = true;
         sendOperations();
      });
   }

   HotRodOperation<?> forceSendOperation(HotRodOperation<?> operation) {
      log.tracef("Immediately sending operation %s to channel %s", operation, channel);
      if (channel.eventLoop().inEventLoop()) {
         actualForceSingleOperation(operation);
      } else {
         channel.eventLoop().submit(() -> actualForceSingleOperation(operation));
      }
      return operation;
   }

   private void actualForceSingleOperation(HotRodOperation<?> operation) {
      assert channel.eventLoop().inEventLoop();
      long messageId = headerDecoder.registerOperation(operation);
      ByteBuf buffer = channel.alloc().buffer();
      codec.writeHeader(buffer, messageId, currentCacheTopologyFunction.apply(operation.getCacheName()), operation);
      operation.writeOperationRequest(channel, buffer, codec);
      channel.writeAndFlush(buffer, channel.voidPromise());
   }

   public void sendOperation(HotRodOperation<?> operation) {
      Channel channel = this.channel;
      if (channel != null) {
         if (operation.forceSend()) {
            forceSendOperation(operation);
            return;
         }
         log.tracef("Enqueueing operation %s to send to channel %s", operation, channel);
      } else {
         log.tracef("Enqueueing operation %s to send to address %s when connected", operation, address);
      }
      queue.offer(operation);
      // TODO: maybe implement a way to only submit if required
      if (channel != null) {
         channel.eventLoop().execute(SEND_OPERATIONS);
      } else {
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
      }
      List<HotRodOperation<?>> channelOps = new ArrayList<>();
      queue.drain(channelOps::add, Integer.MAX_VALUE);
      return channelOps;
   }

   private void sendOperations() {
      assert channel.eventLoop().inEventLoop();
      if (!acceptingRequests || queue.isEmpty()) {
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

   public void close() {
      if (channel != null) {
         channel.close();
      }
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
