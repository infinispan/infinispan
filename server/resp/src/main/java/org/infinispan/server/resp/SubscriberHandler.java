package org.infinispan.server.resp;

import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.encoding.DataConversion;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilter;
import org.infinispan.notifications.cachelistener.filter.EventType;
import org.infinispan.server.resp.logging.Log;
import org.infinispan.util.concurrent.AggregateCompletionStage;
import org.infinispan.util.concurrent.CompletionStages;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.CharsetUtil;

public class SubscriberHandler extends CacheRespRequestHandler {
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass(), Log.class);
   // Random bytes to keep listener keys separate from others. Means `resp|`.
   public static final byte[] PREFIX_CHANNEL_BYTES = new byte[]{114, 101, 115, 112, 124};
   private final Resp3Handler handler;

   public SubscriberHandler(RespServer respServer, Resp3Handler prevHandler) {
      super(respServer);
      this.handler = prevHandler;
   }

   public static byte[] keyToChannel(byte[] keyBytes) {
      byte[] result = new byte[keyBytes.length + PREFIX_CHANNEL_BYTES.length];
      System.arraycopy(PREFIX_CHANNEL_BYTES, 0, result, 0, PREFIX_CHANNEL_BYTES.length);
      System.arraycopy(keyBytes, 0, result, PREFIX_CHANNEL_BYTES.length, keyBytes.length);
      return result;
   }

   public static byte[] channelToKey(byte[] channelBytes) {
      return Arrays.copyOfRange(channelBytes, PREFIX_CHANNEL_BYTES.length, channelBytes.length);
   }

   @Listener(clustered = true)
   static class PubSubListener {
      private final Channel channel;
      private final DataConversion keyConversion;
      private final DataConversion valueConversion;

      PubSubListener(Channel channel, DataConversion keyConversion, DataConversion valueConversion) {
         this.channel = channel;
         this.keyConversion = keyConversion;
         this.valueConversion = valueConversion;
      }

      @CacheEntryCreated
      @CacheEntryModified
      public CompletionStage<Void> onEvent(CacheEntryEvent<Object, Object> entryEvent) {
         byte[] key = channelToKey((byte[]) keyConversion.fromStorage(entryEvent.getKey()));
         byte[] value = (byte[]) valueConversion.fromStorage(entryEvent.getValue());
         if (key.length > 0 && value != null && value.length > 0) {
            // *3 + \r\n + $7 + \r\n + message + \r\n + $ + keylength (log10 + 1) + \r\n + key + \r\n +
            // $ + valuelength (log 10 + 1) + \r\n + value + \r\n
            int byteSize = 2 + 2 + 2 + 2 + 7 + 2 + 1 + (int) Math.log10(key.length) + 1
                  + 2 + key.length + 2 + 1 + (int) Math.log10(value.length) + 1 + 2 + value.length + 2;
            // TODO: this is technically an issue with concurrent events before/after register/unregister message
            ByteBuf byteBuf = channel.alloc().buffer(byteSize, byteSize);
            byteBuf.writeCharSequence("*3\r\n$7\r\nmessage\r\n$" + key.length + "\r\n", CharsetUtil.US_ASCII);
            byteBuf.writeBytes(key);
            byteBuf.writeCharSequence("\r\n$" + value.length + "\r\n", CharsetUtil.US_ASCII);
            byteBuf.writeBytes(value);
            byteBuf.writeByte('\r');
            byteBuf.writeByte('\n');
            assert byteBuf.writerIndex() == byteSize;
            // TODO: add some back pressure? - something like ClientListenerRegistry?
            channel.writeAndFlush(byteBuf, channel.voidPromise());
         }
         return CompletableFutures.completedNull();
      }
   }

   static class ListenerKeyFilter implements CacheEventFilter<Object, Object> {
      private final byte[] key;
      private final DataConversion conversion;

      ListenerKeyFilter(byte[] key, DataConversion conversion) {
         this.key = key;
         this.conversion = conversion;
      }

      @Override
      public boolean accept(Object eventKey, Object oldValue, Metadata oldMetadata, Object newValue,
                            Metadata newMetadata, EventType eventType) {
         return Arrays.equals(key, (byte[]) conversion.fromStorage(eventKey));
      }
   }

   Map<WrappedByteArray, PubSubListener> specificChannelSubscribers = new HashMap<>();

   @Override
   public void handleChannelDisconnect(ChannelHandlerContext ctx) {
      removeAllListeners();
   }

   @Override
   protected CompletionStage<RespRequestHandler> actualHandleRequest(ChannelHandlerContext ctx, RespCommand type, List<byte[]> arguments) {
      initializeIfNecessary(ctx);

      switch (type) {
         case SUBSCRIBE:
            AggregateCompletionStage<Void> aggregateCompletionStage = CompletionStages.aggregateCompletionStage();
            for (byte[] keyChannel : arguments) {
               if (log.isTraceEnabled()) {
                  log.tracef("Subscriber for channel: " + CharsetUtil.UTF_8.decode(ByteBuffer.wrap(keyChannel)));
               }
               WrappedByteArray wrappedByteArray = new WrappedByteArray(keyChannel);
               if (specificChannelSubscribers.get(wrappedByteArray) == null) {
                  PubSubListener pubSubListener = new PubSubListener(ctx.channel(), cache.getKeyDataConversion(), cache.getValueDataConversion());
                  specificChannelSubscribers.put(wrappedByteArray, pubSubListener);
                  byte[] channel = keyToChannel(keyChannel);
                  CompletionStage<Void> stage = cache.addListenerAsync(pubSubListener, new ListenerKeyFilter(channel, cache.getKeyDataConversion()), null);
                  aggregateCompletionStage.dependsOn(handleStageListenerError(stage, keyChannel, true));
               }
            }
            return sendSubscriptions(ctx, aggregateCompletionStage.freeze(), arguments, true);
         case UNSUBSCRIBE:
            aggregateCompletionStage = CompletionStages.aggregateCompletionStage();
            if (arguments.size() == 0) {
               return unsubscribeAll(ctx);
            } else {
               for (byte[] keyChannel : arguments) {
                  WrappedByteArray wrappedByteArray = new WrappedByteArray(keyChannel);
                  PubSubListener listener = specificChannelSubscribers.remove(wrappedByteArray);
                  if (listener != null) {
                     aggregateCompletionStage.dependsOn(handleStageListenerError(cache.removeListenerAsync(listener), keyChannel, false));
                  }
               }
            }
            return sendSubscriptions(ctx, aggregateCompletionStage.freeze(), arguments, false);
         case PING:
            // Note we don't return the handler and just use it to handle the ping - we assume stage is always complete
            handler.handleRequest(ctx, type, arguments);
            break;
         case RESET:
         case QUIT:
            removeAllListeners();
            return handler.handleRequest(ctx, type, arguments);
         case PSUBSCRIBE:
         case PUNSUBSCRIBE:
            RespRequestHandler.stringToByteBuf("-ERR not implemented yet\r\n", allocatorToUse);
            break;
         default:
            return super.actualHandleRequest(ctx, type, arguments);
      }
      return myStage;
   }

   private CompletionStage<Void> handleStageListenerError(CompletionStage<Void> stage, byte[] keyChannel, boolean subscribeOrUnsubscribe) {
      return stage.whenComplete((__, t) -> {
         if (t != null) {
            if (subscribeOrUnsubscribe) {
               log.exceptionWhileRegisteringListener(t, CharsetUtil.UTF_8.decode(ByteBuffer.wrap(keyChannel)));
            } else {
               log.exceptionWhileRemovingListener(t, CharsetUtil.UTF_8.decode(ByteBuffer.wrap(keyChannel)));
            }
         }
      });
   }

   private void removeAllListeners() {
      for (Iterator<Map.Entry<WrappedByteArray, PubSubListener>> iterator = specificChannelSubscribers.entrySet().iterator(); iterator.hasNext(); ) {
         Map.Entry<WrappedByteArray, PubSubListener> entry = iterator.next();
         PubSubListener listener = entry.getValue();
         cache.removeListenerAsync(listener);
         iterator.remove();
      }
   }

   private CompletionStage<RespRequestHandler> unsubscribeAll(ChannelHandlerContext ctx) {
      var aggregateCompletionStage = CompletionStages.aggregateCompletionStage();
      List<byte[]> channels = new ArrayList<>(specificChannelSubscribers.size());
      for (Iterator<Map.Entry<WrappedByteArray, PubSubListener>> iterator = specificChannelSubscribers.entrySet().iterator(); iterator.hasNext(); ) {
         Map.Entry<WrappedByteArray, PubSubListener> entry = iterator.next();
         PubSubListener listener = entry.getValue();
         CompletionStage<Void> stage = cache.removeListenerAsync(listener);
         byte[] keyChannel = entry.getKey().getBytes();
         channels.add(keyChannel);
         aggregateCompletionStage.dependsOn(handleStageListenerError(stage, keyChannel, false));
         iterator.remove();
      }
      return sendSubscriptions(ctx, aggregateCompletionStage.freeze(), channels, false);
   }

   private CompletionStage<RespRequestHandler> sendSubscriptions(ChannelHandlerContext ctx, CompletionStage<Void> stageToWaitFor,
         Collection<byte[]> keyChannels, boolean subscribeOrUnsubscribe) {
      return stageToReturn(stageToWaitFor, ctx, (__, alloc) -> {
         for (byte[] keyChannel : keyChannels) {
            String initialCharSeq = subscribeOrUnsubscribe ? "*2\r\n$9\r\nsubscribe\r\n$" : "*2\r\n$11\r\nunsubscribe\r\n$";

            // Length of string (all ascii so 1 byte per) + (log10 + 1 = sizes of number as char in bytes) + \r\n + bytes themselves + \r\n
            int sizeRequired = initialCharSeq.length() + (int) Math.log10(keyChannel.length) + 1 + 2 + keyChannel.length + 2;
            ByteBuf subscribeBuffer = alloc.apply(sizeRequired);
            int initialPos = subscribeBuffer.writerIndex();
            subscribeBuffer.writeCharSequence(initialCharSeq + keyChannel.length + "\r\n", CharsetUtil.US_ASCII);
            subscribeBuffer.writeBytes(keyChannel);
            subscribeBuffer.writeByte('\r');
            subscribeBuffer.writeByte('\n');
            assert subscribeBuffer.writerIndex() - initialPos == sizeRequired;
         }
      });
   }
}
