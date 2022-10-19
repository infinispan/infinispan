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
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.server.resp.logging.Log;
import org.infinispan.util.concurrent.AggregateCompletionStage;
import org.infinispan.util.concurrent.CompletionStages;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.CharsetUtil;

public class SubscriberHandler extends RespRequestHandler {
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass(), Log.class);
   // Random bytes to keep listener keys separate from others
   public static final byte[] PREFIX_CHANNEL_BYTES = new byte[]{-114, 16, 78, -3, 127};
   private final Resp3Handler handler;
   private final RespServer respServer;

   public SubscriberHandler(RespServer respServer, Resp3Handler prevHandler) {
      this.respServer = respServer;
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

      PubSubListener(Channel channel) {
         this.channel = channel;
      }

      @CacheEntryCreated
      @CacheEntryModified
      public CompletionStage<Void> onEvent(CacheEntryEvent<byte[], byte[]> entryEvent) {
         byte[] key = channelToKey(entryEvent.getKey());
         byte[] value = entryEvent.getValue();
         if (key.length > 0 && value != null && value.length > 0) {
            ByteBuf byteBuf = channel.alloc().buffer(2 + 2
                  + 2 + 7 + 1 + (int) Math.log10(key.length) + 1 + 2
                  + key.length + 2 + 1 + (int) Math.log10(value.length) + 1 + 2 + value.length + 2);
            byteBuf.writeCharSequence("*3\r\n$7\r\nmessage\r\n$" + key.length + "\r\n", CharsetUtil.UTF_8);
            byteBuf.writeBytes(key);
            byteBuf.writeCharSequence("\r\n$" + value.length + "\r\n", CharsetUtil.UTF_8);
            byteBuf.writeBytes(value);
            byteBuf.writeCharSequence("\r\n", CharsetUtil.UTF_8);
            // TODO: add some back pressure? - something like ClientListenerRegistry?
            channel.writeAndFlush(byteBuf);
         }
         return CompletableFutures.completedNull();
      }
   }

   Map<WrappedByteArray, PubSubListener> specificChannelSubscribers = new HashMap<>();

   @Override
   public void handleChannelDisconnect(ChannelHandlerContext ctx) {
      removeAllListeners();
   }

   @Override
   public CompletionStage<RespRequestHandler> handleRequest(ChannelHandlerContext ctx, String type,
         List<byte[]> arguments) {

      switch (type) {
         case "SUBSCRIBE":
            AggregateCompletionStage<Void> aggregateCompletionStage = CompletionStages.aggregateCompletionStage();
            for (byte[] keyChannel : arguments) {
               if (log.isTraceEnabled()) {
                  log.tracef("Subscriber for channel: " + CharsetUtil.UTF_8.decode(ByteBuffer.wrap(keyChannel)));
               }
               WrappedByteArray wrappedByteArray = new WrappedByteArray(keyChannel);
               if (specificChannelSubscribers.get(wrappedByteArray) == null) {
                  PubSubListener pubSubListener = new PubSubListener(ctx.channel());
                  specificChannelSubscribers.put(wrappedByteArray, pubSubListener);
                  byte[] channel = keyToChannel(keyChannel);
                  CompletionStage<Void> stage = respServer.getCache().addListenerAsync(pubSubListener,
                        (key, prevValue, prevMetadata, value, metadata, eventType) -> Arrays.equals(key, channel), null);
                  aggregateCompletionStage.dependsOn(handleStageListenerError(stage, keyChannel, true));
               }
            }
            return sendSubscriptions(ctx, aggregateCompletionStage.freeze(), arguments, true);
         case "UNSUBSCRIBE":
            aggregateCompletionStage = CompletionStages.aggregateCompletionStage();
            if (arguments.size() == 0) {
               return unsubscribeAll(ctx);
            } else {
               for (byte[] keyChannel : arguments) {
                  WrappedByteArray wrappedByteArray = new WrappedByteArray(keyChannel);
                  PubSubListener listener = specificChannelSubscribers.remove(wrappedByteArray);
                  if (listener != null) {
                     aggregateCompletionStage.dependsOn(handleStageListenerError(respServer.getCache().removeListenerAsync(listener), keyChannel, false));
                  }
               }
            }
            return sendSubscriptions(ctx, aggregateCompletionStage.freeze(), arguments, false);
         case "PING":
            // Note we don't return the handler and just use it to handle the ping - we assume stage is always complete
            handler.handleRequest(ctx, type, arguments);
            break;
         case "RESET":
         case "QUIT":
            removeAllListeners();
            return handler.handleRequest(ctx, type, arguments);
         case "PSUBSCRIBE":
         case "PUNSUBSCRIBE":
            ctx.writeAndFlush(RespRequestHandler.stringToByteBuf("-ERR not implemented yet\r\n", ctx.alloc()));
            break;
         default:
            return super.handleRequest(ctx, type, arguments);
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
         respServer.getCache().removeListenerAsync(listener);
         iterator.remove();
      }
   }

   private CompletionStage<RespRequestHandler> unsubscribeAll(ChannelHandlerContext ctx) {
      var aggregateCompletionStage = CompletionStages.aggregateCompletionStage();
      List<byte[]> channels = new ArrayList<>(specificChannelSubscribers.size());
      for (Iterator<Map.Entry<WrappedByteArray, PubSubListener>> iterator = specificChannelSubscribers.entrySet().iterator(); iterator.hasNext(); ) {
         Map.Entry<WrappedByteArray, PubSubListener> entry = iterator.next();
         PubSubListener listener = entry.getValue();
         CompletionStage<Void> stage = respServer.getCache().removeListenerAsync(listener);
         byte[] keyChannel = entry.getKey().getBytes();
         channels.add(keyChannel);
         aggregateCompletionStage.dependsOn(handleStageListenerError(stage, keyChannel, false));
         iterator.remove();
      }
      return sendSubscriptions(ctx, aggregateCompletionStage.freeze(), channels, false);
   }

   private CompletionStage<RespRequestHandler> sendSubscriptions(ChannelHandlerContext ctx, CompletionStage<Void> stageToWaitFor,
         Collection<byte[]> keyChannels, boolean subscribeOrUnsubscribe) {
      return stageToReturn(stageToWaitFor, ctx, (__, t) -> {
         if (t != null) {
            if (subscribeOrUnsubscribe) {
               ctx.writeAndFlush("-ERR Failure adding client listener");
            } else {
               ctx.writeAndFlush("-ERR Failure unsubscribing client listener");
            }
            return;
         }
         for (byte[] keyChannel : keyChannels) {
            int bufferCap = subscribeOrUnsubscribe ? 20 : 22;
            String initialCharSeq = subscribeOrUnsubscribe ? "*2\r\n$9\r\nsubscribe\r\n$" : "*2\r\n$11\r\nunsubscribe\r\n$";

            ByteBuf subscribeBuffer = ctx.alloc().buffer(bufferCap + (int) Math.log10(keyChannel.length) + 1 + keyChannel.length + 2);
            subscribeBuffer.writeCharSequence(initialCharSeq + keyChannel.length + "\r\n", CharsetUtil.UTF_8);
            subscribeBuffer.writeBytes(keyChannel);
            subscribeBuffer.writeCharSequence("\r\n", CharsetUtil.UTF_8);
            ctx.writeAndFlush(subscribeBuffer);
         }
      });
   }
}
