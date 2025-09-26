package org.infinispan.client.hotrod.impl.transport.netty;

import static org.infinispan.client.hotrod.impl.protocol.HotRodConstants.CACHE_ENTRY_CREATED_EVENT_RESPONSE;
import static org.infinispan.client.hotrod.impl.protocol.HotRodConstants.CACHE_ENTRY_EXPIRED_EVENT_RESPONSE;
import static org.infinispan.client.hotrod.impl.protocol.HotRodConstants.CACHE_ENTRY_MODIFIED_EVENT_RESPONSE;
import static org.infinispan.client.hotrod.impl.protocol.HotRodConstants.CACHE_ENTRY_REMOVED_EVENT_RESPONSE;
import static org.infinispan.client.hotrod.impl.protocol.HotRodConstants.COUNTER_EVENT_RESPONSE;
import static org.infinispan.client.hotrod.logging.Log.HOTROD;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.ProtocolVersion;
import org.infinispan.client.hotrod.configuration.ClientIntelligence;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.counter.impl.HotRodCounterEvent;
import org.infinispan.client.hotrod.event.impl.AbstractClientEvent;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.client.hotrod.impl.operations.AddClientListenerOperation;
import org.infinispan.client.hotrod.impl.operations.ByteBufCacheUnmarshaller;
import org.infinispan.client.hotrod.impl.operations.HotRodOperation;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.ArrayRingBuffer;
import org.infinispan.commons.util.Util;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.Signal;

public class HeaderDecoder extends HintedReplayingDecoder<HeaderDecoder.State> {
   private static final Log log = LogFactory.getLog(HeaderDecoder.class);
   // used for HeaderOrEventDecoder, too, as the function is similar
   public static final String NAME = "header-decoder";
   private final Configuration configuration;
   private final TimeService timeService;
   private final OperationDispatcher dispatcher;
   private final List<byte[]> listeners = new ArrayList<>();
   // Marshaller is shared for the entire connection and swaps out DataFormat and ByteBuf per request
   private final ByteBufCacheUnmarshaller unmarshaller;
   private volatile boolean closing;

   private final ArrayRingBuffer<OperationTimeout> operations = new ArrayRingBuffer<>(32);
   private final Runnable CHECK_TIMEOUTS = this::checkForTimeouts;

   // All the following can only be accessed while in the event loop of the channel
   private Channel channel;

   private HotRodOperation<?> operation;
   private short status;
   private long receivedMessageId;
   private short receivedOpCode;

   private long messageOffset;
   private Codec codec;

   private ScheduledFuture<?> scheduledTimeout;

   // The next two instance variables are only for when a custom timeout is used
   // This is a ConcurrentHashMap solely for methods reading incomplete. Writing to the map is done solely
   // in the event loop
   private final Map<Long, HotRodOperation<?>> incomplete = new ConcurrentHashMap<>();
   private final Map<Long, ScheduledFuture<?>> timeouts = new HashMap<>();

   record OperationTimeout(HotRodOperation<?> op, long timeout) { }

   public HeaderDecoder(Configuration configuration, OperationDispatcher dispatcher) {
      super(State.READ_MESSAGE_ID);
      this.configuration = configuration;
      this.timeService = dispatcher.getTimeService();
      this.dispatcher = dispatcher;
      this.unmarshaller = new ByteBufCacheUnmarshaller(configuration.getClassAllowList());
   }

   public Configuration getConfiguration() {
      return configuration;
   }

   public Channel getChannel() {
      assert channel == null || channel.eventLoop().inEventLoop();
      return channel;
   }

   private void checkForTimeouts() {
      long currentTime = timeService.time();
      while (true) {
         OperationTimeout opTimeout = operations.peek();
         if (opTimeout == null) {
            log.trace("No operations left, not scheduling timeout checker");
            scheduledTimeout = null;
            break;
         }
         long nanoDiff = opTimeout.timeout - currentTime;
         if (nanoDiff < 0) {
            long messageId = operations.getHeadSequence();
            // Remove the polled one now
            operations.poll();
            // Insert the operation back into the HashMap so we can continue using the RingBuffer for timeouts
            // This way when a response is processed for the timed out operation we can process the bytes properly
            incomplete.put(messageId, opTimeout.op);
            dispatcher.handleResponse(opTimeout.op, messageId, channel, null,
                  new SocketTimeoutException(opTimeout.op + " timed out after " + configuration.socketTimeout() + " ms"));
         } else {
            log.tracef("Rescheduling timeout checker for ~%d ms", TimeUnit.NANOSECONDS.toMillis(nanoDiff));
            channel.eventLoop().schedule(CHECK_TIMEOUTS, nanoDiff, TimeUnit.NANOSECONDS);
            break;
         }
      }

   }

   @Override
   public boolean isSharable() {
      return false;
   }

   public long registerOperation(HotRodOperation<?> operation) {
      log.tracef("Decoder is %s Channel is %s for operation %s", this, channel, operation);
      assert channel.eventLoop().inEventLoop();

      long messageId = messageOffset++;

      if (log.isTraceEnabled()) {
         log.tracef("Registering id %d for operation %s(%08X) on %s",
               messageId, operation, System.identityHashCode(operation), channel);
      }
      if (closing) {
         HotRodClientException noOpException = HOTROD.noMoreOperationsAllowed();
         dispatcher.handleResponse(operation, messageId, channel, null, noOpException);
         throw noOpException;
      }
      long timeout = operation.timeout();
      // Custom timeouts do not work with the RingBuffer due to not ordering based on timeout
      // AddClientListenerOperation can delay its timeout so we also don't allow it
      if (timeout > 0 || operation.isInstanceOf(AddClientListenerOperation.class)) {
         Long messageIdLong = messageId;
         HotRodOperation<?> prev = incomplete.put(messageIdLong, operation);
         assert prev == null;
         scheduleTimeout(operation, messageIdLong);
      } else {
         long nanoTime = timeService.time();
         int socketTimeout = configuration.socketTimeout();
         operations.set(messageId, new OperationTimeout(operation, nanoTime + TimeUnit.MILLISECONDS.toNanos(socketTimeout)));

         if (scheduledTimeout == null) {
            log.tracef("Scheduling timeout checker for %d ms", socketTimeout);
            scheduledTimeout = channel.eventLoop().schedule(CHECK_TIMEOUTS, socketTimeout, TimeUnit.MILLISECONDS);
         }
      }

      return messageId;
   }

   private void scheduleTimeout(HotRodOperation<?> op, Long messageIdLong) {
      long timeout = op.timeout() > 0 ? op.timeout() : configuration.socketTimeout();
      log.tracef("Scheduling timeout for %d ms", timeout);
      ScheduledFuture<?> future = channel.eventLoop().schedule(() -> {
         timeouts.remove(messageIdLong);
         dispatcher.handleResponse(op, messageIdLong, channel, null,
               new SocketTimeoutException(this + " timed out after " + configuration.socketTimeout() + " ms"));
      }, timeout, TimeUnit.MILLISECONDS);
      timeouts.put(messageIdLong, future);
   }

   public void refreshTimeout(HotRodOperation<?> op, long messageId) {
      // Currently only supported for AddClientListenerOperation
      assert op.isInstanceOf(AddClientListenerOperation.class);
      Long messageIdLong = messageId;
      ScheduledFuture<?> future = timeouts.remove(messageIdLong);
      if (future == null) {
         log.tracef("Unable to refresh timeout for messageID %d", messageId);
         return;
      }

      log.tracef("Refreshing timeout with id %d for op %s", messageId, op);

      future.cancel(false);
      scheduleTimeout(op, messageIdLong);
   }

   @Override
   public void channelActive(ChannelHandlerContext ctx) throws Exception {
      channel = ctx.channel();
      log.tracef("Decoder %s has Channel %s active", this, channel);
      if (codec == null) {
         codec = configuration.version().getCodec();
      }
      super.channelActive(ctx);
   }

   private HotRodOperation<?> removeOperation(long messageId) {
      OperationTimeout opTimeout = operations.remove(messageId);
      if (opTimeout != null) {
         return opTimeout.op;
      }
      HotRodOperation<?> op = incomplete.remove(messageId);
      ScheduledFuture<?> future = timeouts.remove(messageId);
      if (future != null) {
         future.cancel(false);
      }
      return op;
   }

   @Override
   protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
      try {
         switch (state()) {
            case READ_MESSAGE_ID:
               // If we misread some bytes here we don't want to attribute it to an operation
               operation = null;
               receivedMessageId = codec.readMessageId(in);
               receivedOpCode = in.readUnsignedByte();
               switch (receivedOpCode) {
                  case CACHE_ENTRY_CREATED_EVENT_RESPONSE, CACHE_ENTRY_MODIFIED_EVENT_RESPONSE, CACHE_ENTRY_REMOVED_EVENT_RESPONSE, CACHE_ENTRY_EXPIRED_EVENT_RESPONSE -> {
                     operation = receivedMessageId == 0 ? null : incomplete.get(receivedMessageId);
                     // The operation may be null even if the messageId was set: the server does not really wait
                     // until all events are sent, only until these are queued. In such case the operation may
                     // complete earlier.
                     if (operation != null && !(operation.isInstanceOf(AddClientListenerOperation.class))) {
                        throw HOTROD.operationIsNotAddClientListener(receivedMessageId, operation.toString());
                     } else if (log.isTraceEnabled()) {
                        log.tracef("Received event for request %d", receivedMessageId, operation);
                     }
                     checkpoint(State.READ_CACHE_EVENT);
                     // the loop in HintedReplayingDecoder will call decode again
                     return;
                  }
                  case COUNTER_EVENT_RESPONSE -> {
                     checkpoint(State.READ_COUNTER_EVENT);
                     // the loop in HintedReplayingDecoder will call decode again
                     return;
                  }
               }
               if (receivedMessageId >= 0) {
                  // we can remove the operation at this point since we'll read no more in this state
                  operation = removeOperation(receivedMessageId);
                  if (operation == null) {
                     throw HOTROD.unknownMessageId(receivedMessageId);
                  }
               }
               if (log.isTraceEnabled()) {
                  log.tracef("Received response for request %d, %s", receivedMessageId, operation);
               }
               checkpoint(State.READ_STATUS);
               // fall through
            case READ_STATUS:
               if (log.isTraceEnabled()) {
                  log.tracef("Decoding header for message %s", HotRodConstants.Names.of(receivedOpCode));
               }
               status = in.readUnsignedByte();
               checkpoint(State.READ_TOPOLOGY);
               // fall through
            case READ_TOPOLOGY:
               short topologyChangeByte = in.readUnsignedByte();
               if (topologyChangeByte == 1) {
                  readNewTopologyAndHash(in, operation.getCacheName());
               }
               checkpoint(State.READ_PAYLOAD);
               // fall through
            case READ_PAYLOAD:
               // Now that all headers values have been read, check the error responses.
               // This avoids situations where an exceptional return ends up with
               // the socket containing data from previous request responses.
               if (operation == null || receivedOpCode != operation.responseOpCode()) {
                  String cacheName = operation == null ? "" : operation.getCacheName();
                  short responseCode = operation == null ? -1 : operation.responseOpCode();
                  if (receivedOpCode == HotRodConstants.ERROR_RESPONSE) {
                     codec.checkForErrorsInResponseStatus(in, cacheName, receivedMessageId, status, channel.remoteAddress());
                  }
                  throw HOTROD.invalidResponse(cacheName, responseCode, receivedOpCode);
               }
               if (log.isTraceEnabled()) {
                  log.tracef("Decoding payload for message %s", HotRodConstants.Names.of(receivedOpCode));
               }
               try {
                  unmarshaller.setDataFormat(operation.getDataFormat());
                  Object resp = operation.createResponse(in, status, this, codec, unmarshaller);
                  dispatcher.handleResponse((HotRodOperation<Object>) operation, receivedMessageId, ctx.channel(), resp, null);
               } catch (Signal signal) {
                  throw signal;
               } catch (Throwable t) {
                  dispatcher.handleResponse(operation, receivedMessageId, ctx.channel(), null, t);
               }
               checkpoint(State.READ_MESSAGE_ID);
               break;
            case READ_CACHE_EVENT:
               if (log.isTraceEnabled()) {
                  log.tracef("Decoding cache event %s", HotRodConstants.Names.of(receivedOpCode));
               }
               AbstractClientEvent cacheEvent;
               try {
                  cacheEvent = codec.readCacheEvent(in, receivedMessageId, dispatcher.getClientListenerNotifier()::getCacheDataFormat,
                        receivedOpCode, configuration.getClassAllowList(), ctx.channel().remoteAddress());
               } catch (Signal signal) {
                  throw signal;
               } catch (Throwable t) {
                  log.unableToReadEventFromServer(t, ctx.channel().remoteAddress());
                  throw t;
               }
               if (operation != null && operation.isInstanceOf(AddClientListenerOperation.class)) {
                  refreshTimeout(operation, receivedMessageId);
               }
               invokeEvent(cacheEvent.getListenerId(), cacheEvent);
               checkpoint(State.READ_MESSAGE_ID);
               break;
            case READ_COUNTER_EVENT:
               if (log.isTraceEnabled()) {
                  log.tracef("Decoding counter event %s", HotRodConstants.Names.of(receivedOpCode));
               }
               HotRodCounterEvent counterEvent;
               try {
                  counterEvent = codec.readCounterEvent(in);
               } catch (Signal signal) {
                  throw signal;
               } catch (Throwable t) {
                  HOTROD.unableToReadEventFromServer(t, ctx.channel().remoteAddress());
                  throw t;
               }
               invokeEvent(counterEvent.getListenerId(), counterEvent);
               checkpoint(State.READ_MESSAGE_ID);
               break;
         }
      } catch (Exception e) {
         // Don't set the operation to null as we probably want to set it in exceptionCaught
         // If this is server error make sure to restart the state of decoder
         checkpoint(State.READ_MESSAGE_ID);
         throw e;
      }
   }

   private void readNewTopologyAndHash(ByteBuf buf, String cacheName) {
      int newTopologyId = ByteBufUtil.readVInt(buf);

      InetSocketAddress[] addresses = readTopology(buf);

      final short hashFunctionVersion;
      final SocketAddress[][] segmentOwners;
      if (dispatcher.getClientIntelligence().getValue() == ClientIntelligence.HASH_DISTRIBUTION_AWARE.getValue()) {
         // Only read the hash if we asked for it
         hashFunctionVersion = buf.readUnsignedByte();
         int numSegments = ByteBufUtil.readVInt(buf);
         segmentOwners = new SocketAddress[numSegments][];
         if (hashFunctionVersion > 0) {
            for (int i = 0; i < numSegments; i++) {
               short numOwners = buf.readUnsignedByte();
               segmentOwners[i] = new SocketAddress[numOwners];
               for (int j = 0; j < numOwners; j++) {
                  int memberIndex = ByteBufUtil.readVInt(buf);
                  segmentOwners[i][j] = addresses[memberIndex];
               }
            }
         }
      } else {
         hashFunctionVersion = -1;
         segmentOwners = null;
      }

      // Only update the topology if this channel is fully connected and authenticated
      OperationChannel operationChannel = channel.attr(OperationChannel.OPERATION_CHANNEL_ATTRIBUTE_KEY).get();
      if (operationChannel != null && operationChannel.isAcceptingRequests()) {
         dispatcher.updateTopology(cacheName, operation, newTopologyId,
               addresses, segmentOwners, hashFunctionVersion);
      }
   }

   private InetSocketAddress[] readTopology(ByteBuf buf) {
      int clusterSize = ByteBufUtil.readVInt(buf);
      InetSocketAddress[] addresses = new InetSocketAddress[clusterSize];
      for (int i = 0; i < clusterSize; i++) {
         String host = ByteBufUtil.readString(buf);
         int port = buf.readUnsignedShort();
         addresses[i] = InetSocketAddress.createUnresolved(host, port);
      }
      return addresses;
   }

   public void setCodec(Codec codec) {
      assert channel.eventLoop().inEventLoop();
      if (configuration.version() == ProtocolVersion.PROTOCOL_VERSION_AUTO) {
         // Here for the purpose of tests to override explicitly as needed
         if (codec == null) {
            this.codec = codec;
         }
         channel.attr(OperationChannel.OPERATION_CHANNEL_ATTRIBUTE_KEY).get().setCodec(codec);
      }
   }

   private void invokeEvent(byte[] listenerId, Object cacheEvent) {
      try {
         dispatcher.getClientListenerNotifier().invokeEvent(listenerId, cacheEvent);
      } catch (Exception e) {
         HOTROD.unexpectedErrorConsumingEvent(cacheEvent, e);
      }
   }

   @Override
   public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      if (operation != null && !operation.asCompletableFuture().isDone()) {
         HotRodOperation<?> op = operation;
         // we null out the operation, just in case the response handling generates another exception
         operation = null;
         dispatcher.handleResponse(op, receivedMessageId, ctx.channel(), null, cause);
      } else {
         TransportException transportException = log.errorFromUnknownOperation(ctx.channel(), cause, ctx.channel().remoteAddress());
         if (log.isTraceEnabled()) {
            log.tracef(transportException, "Requesting %s close due to exception", ctx.channel());
         }
         handleClosing(ctx, transportException);
         ctx.close();
      }
   }

   @Override
   public void channelInactive(ChannelHandlerContext ctx) throws Exception {
      TransportException transportException = log.connectionClosed(channel.remoteAddress(), channel.remoteAddress());
      handleClosing(ctx, transportException);
      super.channelInactive(ctx);
   }

   void failoverClientListeners() {
      for (byte[] listenerId : listeners) {
         dispatcher.getClientListenerNotifier().failoverClientListener(listenerId);
      }
   }

   private void handleClosing(ChannelHandlerContext ctx, Throwable t) {
      if (closing) {
         return;
      }
      assert channel == null || channel.eventLoop().inEventLoop();
      closing = true;
      dispatcher.handleChannelFailure(ctx.channel(), t);
      operations.forEach((opTimeout, id) -> {
         try {
            dispatcher.handleResponse(opTimeout.op, id, ctx.channel(), null, t);
         } catch (Throwable innerT) {
            HOTROD.errorf(t, "Failed to complete %s", opTimeout.op);
         }
      });
      operations.clear();
      for (Map.Entry<Long, HotRodOperation<?>> entry : incomplete.entrySet()) {
         HotRodOperation<?> op = entry.getValue();
         try {
            dispatcher.handleResponse(op, entry.getKey(), ctx.channel(), null, t);
         } catch (Throwable innerT) {
            HOTROD.errorf(t, "Failed to complete %s", op);
         }
         ScheduledFuture<?> f = timeouts.remove(entry.getKey());
         if (f != null) {
            f.cancel(false);
         }
      }
      failoverClientListeners();
      incomplete.clear();
   }

   /**
    * {@inheritDoc}
    *
    * Checkpoint is exposed for implementations of {@link HotRodOperation}
    */
   @Override
   public void checkpoint() {
      super.checkpoint();
   }

   public Map<Long, HotRodOperation<?>> registeredOperationsById() {
      var map = new HashMap<Long, HotRodOperation<?>>();
      operations.forEach((opTimeout, id) -> {
         map.put(id, opTimeout.op);
      });

      map.putAll(incomplete);

      return map;
   }

   public void addListener(byte[] listenerId) {
      if (log.isTraceEnabled()) {
         log.tracef("Decoder %08X adding listener %s", hashCode(), Util.printArray(listenerId));
      }
      listeners.add(listenerId);
   }

   // must be called from event loop thread!
   public void removeListener(byte[] listenerId) {
      boolean removed = listeners.removeIf(id -> Arrays.equals(id, listenerId));
      if (log.isTraceEnabled()) {
         log.tracef("Decoder %08X removed? %s listener %s", hashCode(), Boolean.toString(removed), Util.printArray(listenerId));
      }
   }

   enum State {
      READ_MESSAGE_ID,
      READ_STATUS,
      READ_TOPOLOGY,
      READ_PAYLOAD,
      READ_CACHE_EVENT, READ_COUNTER_EVENT,
   }
}
