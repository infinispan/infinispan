package org.infinispan.server.resp;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.server.resp.commands.PubSubResp3Command;
import org.infinispan.server.resp.commands.pubsub.KeyChannelUtils;
import org.infinispan.server.resp.commands.pubsub.RespCacheListener;
import org.infinispan.server.resp.logging.Log;
import org.infinispan.server.resp.meta.ClientMetadata;
import org.infinispan.server.resp.serialization.Resp3Type;
import org.infinispan.server.resp.serialization.bytebuf.ByteBufResponseWriter;
import org.infinispan.server.resp.serialization.bytebuf.ByteBufferUtils;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.AttributeKey;
import io.netty.util.CharsetUtil;

public class SubscriberHandler extends CacheRespRequestHandler {
   private static final Log log = Log.getLog(SubscriberHandler.class);
   private static final AttributeKey<Long> SUBSCRIPTIONS_COUNTER = AttributeKey.newInstance("channel-subscriptions");
   private final Resp3Handler resp3Handler;

   public SubscriberHandler(RespServer respServer, Resp3Handler prevHandler) {
      super(respServer, prevHandler.cache());
      this.resp3Handler = prevHandler;
   }

   public static RespCacheListener newKeyListener(Channel channel, byte[] key) {
      return new PubSubListener(channel, key);
   }

   public static RespCacheListener newPatternListener(Channel channel, byte[] pattern) {
      return new PubSubListener(channel, null, pattern);
   }

   @Listener(clustered = true)
   public static class PubSubListener implements RespCacheListener {
      private final Channel channel;
      private final byte[] key;
      private final byte[] pattern;

      private PubSubListener(Channel channel, byte[] key) {
         this(channel, key, null);
      }

      private PubSubListener(Channel channel, byte[] key, byte[] pattern) {
         this.channel = channel;
         this.key = key;
         this.pattern = pattern;
      }

      @CacheEntryCreated
      @CacheEntryModified
      public CompletionStage<Void> onEvent(CacheEntryEvent<Object, byte[]> entryEvent) {
         byte[] key = KeyChannelUtils.channelToKey(unwrap(entryEvent.getKey()));
         byte[] value = entryEvent.getValue();
         if (key.length > 0 && value != null && value.length > 0) {
            List<Object> elements;
            int byteSize;

            if (pattern != null) {
               // *4\r\n + $8\r\npmessage\r\n + $<patlen>\r\n<pattern>\r\n + $<keylen>\r\n<key>\r\n + $<vallen>\r\n<value>\r\n
               elements = List.of(PubSubEvents.PMESSAGE, pattern, key, value);
               byteSize = 2 + 2 + 2 + 2 + 8 + 2
                     + 1 + ByteBufferUtils.stringSize(pattern.length) + 2 + pattern.length + 2
                     + 1 + ByteBufferUtils.stringSize(key.length) + 2 + key.length + 2
                     + 1 + ByteBufferUtils.stringSize(value.length) + 2 + value.length + 2;
            } else {
               // *3\r\n + $7\r\nmessage\r\n + $<keylen>\r\n<key>\r\n + $<vallen>\r\n<value>\r\n
               elements = List.of(PubSubEvents.MESSAGE, key, value);
               byteSize = 2 + 2 + 2 + 2 + 7 + 2
                     + 1 + ByteBufferUtils.stringSize(key.length) + 2 + key.length + 2
                     + 1 + ByteBufferUtils.stringSize(value.length) + 2 + value.length + 2;
            }

            // TODO: this is technically an issue with concurrent events before/after register/unregister message
            ByteBuf byteBuf = channel.alloc().buffer(byteSize, byteSize);
            ByteBufPool allocator = ignore -> byteBuf;
            ByteBufResponseWriter w = new ByteBufResponseWriter(allocator);
            w.array(elements, Resp3Type.BULK_STRING);
            assert byteBuf.writerIndex() == byteSize;
            // TODO: add some back pressure? - something like ClientListenerRegistry?
            channel.writeAndFlush(byteBuf, channel.voidPromise());
         }
         return CompletableFutures.completedNull();
      }

      private byte[] unwrap(Object key) {
         return key instanceof WrappedByteArray
               ? ((WrappedByteArray) key).getBytes()
               : (byte[]) key;
      }

      @Override
      public byte[] subscribedChannel() {
         return key;
      }

      @Override
      public byte[] pattern() {
         return pattern;
      }
   }

   private final Map<WrappedByteArray, RespCacheListener> specificChannelSubscribers = new HashMap<>();
   private final Map<WrappedByteArray, RespCacheListener> patternSubscribers = new HashMap<>();

   public Map<WrappedByteArray, RespCacheListener> specificChannelSubscribers() {
      return specificChannelSubscribers;
   }

   public Map<WrappedByteArray, RespCacheListener> patternSubscribers() {
      return patternSubscribers;
   }

   public Resp3Handler resp3Handler() {
      return resp3Handler;
   }

   @Override
   public void handleChannelDisconnect(ChannelHandlerContext ctx) {
      removeAllListeners();
   }

   @Override
   protected CompletionStage<RespRequestHandler> actualHandleRequest(ChannelHandlerContext ctx, RespCommand command, List<byte[]> arguments) {
      initializeIfNecessary(ctx);
      if (command instanceof PubSubResp3Command pubSubsCommand) {
         return pubSubsCommand.perform(this, ctx, arguments);
      }
      return super.actualHandleRequest(ctx, command, arguments);
   }

   public CompletionStage<Void> handleStageListenerError(CompletionStage<Void> stage, byte[] keyChannel, boolean subscribeOrUnsubscribe) {
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

   public void removeAllListeners() {
      removeAllFrom(specificChannelSubscribers);
      removeAllFrom(patternSubscribers);
   }

   private void removeAllFrom(Map<WrappedByteArray, RespCacheListener> subscribers) {
      for (Iterator<Map.Entry<WrappedByteArray, RespCacheListener>> iterator = subscribers.entrySet().iterator(); iterator.hasNext(); ) {
         Map.Entry<WrappedByteArray, RespCacheListener> entry = iterator.next();
         cache().removeListenerAsync(entry.getValue());
         iterator.remove();
      }
   }

   public CompletionStage<RespRequestHandler> unsubscribeAll(ChannelHandlerContext ctx) {
      return unsubscribeAllFrom(ctx, specificChannelSubscribers, false);
   }

   public CompletionStage<RespRequestHandler> punsubscribeAll(ChannelHandlerContext ctx) {
      return unsubscribeAllFrom(ctx, patternSubscribers, true);
   }

   private CompletionStage<RespRequestHandler> unsubscribeAllFrom(ChannelHandlerContext ctx,
                                                                   Map<WrappedByteArray, RespCacheListener> subscribers,
                                                                   boolean isPattern) {
      ClientMetadata metadata = respServer().metadataRepository().client();
      var aggregateCompletionStage = CompletionStages.aggregateCompletionStage();
      List<byte[]> channels = new ArrayList<>(subscribers.size());
      for (Iterator<Map.Entry<WrappedByteArray, RespCacheListener>> iterator = subscribers.entrySet().iterator(); iterator.hasNext(); ) {
         Map.Entry<WrappedByteArray, RespCacheListener> entry = iterator.next();
         RespCacheListener listener = entry.getValue();
         CompletionStage<Void> stage = cache().removeListenerAsync(listener);
         byte[] keyChannel = entry.getKey().getBytes();
         channels.add(keyChannel);
         aggregateCompletionStage.dependsOn(handleStageListenerError(stage, keyChannel, false));
         iterator.remove();
         metadata.decrementPubSubClients();
      }
      return sendSubscriptions(ctx, aggregateCompletionStage.freeze(), channels, false, isPattern);
   }

   public CompletionStage<RespRequestHandler> sendSubscriptions(ChannelHandlerContext ctx, CompletionStage<Void> stageToWaitFor,
                                                                Collection<byte[]> keyChannels, boolean isSubscribe) {
      return sendSubscriptions(ctx, stageToWaitFor, keyChannels, isSubscribe, false);
   }

   public CompletionStage<RespRequestHandler> sendSubscriptions(ChannelHandlerContext ctx, CompletionStage<Void> stageToWaitFor,
                                                                Collection<byte[]> keyChannels, boolean isSubscribe, boolean isPattern) {
      return stageToReturn(stageToWaitFor, ctx, (__, alloc) -> {
         assert ctx.executor().inEventLoop();

         Long counter = ctx.channel().attr(SUBSCRIPTIONS_COUNTER).get();
         if (counter == null) counter = 0L;

         // PubSub events require the object type to be a bulk string.
         byte[] type;
         if (isSubscribe) {
            type = isPattern ? PubSubEvents.PSUBSCRIBE : PubSubEvents.SUBSCRIBE;
         } else {
            type = isPattern ? PubSubEvents.PUNSUBSCRIBE : PubSubEvents.UNSUBSCRIBE;
         }
         for (byte[] keyChannel : keyChannels) {
            counter = Math.max(0, counter + (isSubscribe ? 1 : -1));
            long c = counter;
            writer.array(List.of(type, keyChannel, c), (o, w) -> {
               if (o instanceof byte[]) {
                  w.string((byte[]) o);
               } else {
                  w.integers((Number) o);
               }
            });
         }

         if (counter == 0) {
            ctx.channel().attr(SUBSCRIPTIONS_COUNTER).set(null);
         } else {
            ctx.channel().attr(SUBSCRIPTIONS_COUNTER).set(counter);
         }
      });
   }

   /**
    * Describe which type of event is present on the pub sub message.
    * <p>
    * The type of event is the first element in the array. It <b>must</b> be a bulk string.
    * </p>
    */
   static final class PubSubEvents {
      static final byte[] SUBSCRIBE = "subscribe".getBytes(StandardCharsets.US_ASCII);
      static final byte[] UNSUBSCRIBE = "unsubscribe".getBytes(StandardCharsets.US_ASCII);
      static final byte[] MESSAGE = "message".getBytes(StandardCharsets.US_ASCII);
      static final byte[] PSUBSCRIBE = "psubscribe".getBytes(StandardCharsets.US_ASCII);
      static final byte[] PUNSUBSCRIBE = "punsubscribe".getBytes(StandardCharsets.US_ASCII);
      static final byte[] PMESSAGE = "pmessage".getBytes(StandardCharsets.US_ASCII);
   }
}
