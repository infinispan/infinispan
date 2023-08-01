package org.infinispan.server.resp;

import static org.infinispan.server.resp.RespConstants.CRLF;
import static org.infinispan.server.resp.RespConstants.CRLF_STRING;

import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.util.ArrayList;
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
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.server.resp.commands.PubSubResp3Command;
import org.infinispan.server.resp.commands.pubsub.KeyChannelUtils;
import org.infinispan.server.resp.logging.Log;
import org.infinispan.util.concurrent.CompletionStages;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.AttributeKey;
import io.netty.util.CharsetUtil;

public class SubscriberHandler extends CacheRespRequestHandler {
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass(), Log.class);
   private static final AttributeKey<Long> SUBSCRIPTIONS_COUNTER = AttributeKey.newInstance("channel-subscriptions");
   private final Resp3Handler resp3Handler;

   public SubscriberHandler(RespServer respServer, Resp3Handler prevHandler) {
      super(respServer);
      this.resp3Handler = prevHandler;
   }

   @Listener(clustered = true)
   public static class PubSubListener {
      private final Channel channel;
      private final DataConversion keyConversion;
      private final DataConversion valueConversion;

      public PubSubListener(Channel channel, DataConversion keyConversion, DataConversion valueConversion) {
         this.channel = channel;
         this.keyConversion = keyConversion;
         this.valueConversion = valueConversion;
      }

      @CacheEntryCreated
      @CacheEntryModified
      public CompletionStage<Void> onEvent(CacheEntryEvent<Object, Object> entryEvent) {
         byte[] key = KeyChannelUtils.channelToKey((byte[]) keyConversion.fromStorage(entryEvent.getKey()));
         byte[] value = (byte[]) valueConversion.fromStorage(entryEvent.getValue());
         if (key.length > 0 && value != null && value.length > 0) {
            // *3 + \r\n + $7 + \r\n + message + \r\n + $ + keylength (log10 + 1) + \r\n + key + \r\n +
            // $ + valuelength (log 10 + 1) + \r\n + value + \r\n
            int byteSize = 2 + 2 + 2 + 2 + 7 + 2 + 1 + (int) Math.log10(key.length) + 1
                  + 2 + key.length + 2 + 1 + (int) Math.log10(value.length) + 1 + 2 + value.length + 2;
            // TODO: this is technically an issue with concurrent events before/after register/unregister message
            ByteBuf byteBuf = channel.alloc().buffer(byteSize, byteSize);
            byteBuf.writeCharSequence("*3\r\n$7\r\nmessage\r\n$" + key.length + CRLF_STRING, CharsetUtil.US_ASCII);
            byteBuf.writeBytes(key);
            byteBuf.writeCharSequence("\r\n$" + value.length + CRLF_STRING, CharsetUtil.US_ASCII);
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

   private final Map<WrappedByteArray, PubSubListener> specificChannelSubscribers = new HashMap<>();

   public Map<WrappedByteArray, PubSubListener> specificChannelSubscribers() {
      return specificChannelSubscribers;
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
      if (command instanceof PubSubResp3Command) {
         PubSubResp3Command pubSubsCommand = (PubSubResp3Command) command;
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
      for (Iterator<Map.Entry<WrappedByteArray, PubSubListener>> iterator = specificChannelSubscribers.entrySet().iterator(); iterator.hasNext(); ) {
         Map.Entry<WrappedByteArray, PubSubListener> entry = iterator.next();
         PubSubListener listener = entry.getValue();
         cache.removeListenerAsync(listener);
         iterator.remove();
      }
   }

   public CompletionStage<RespRequestHandler> unsubscribeAll(ChannelHandlerContext ctx) {
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

   public CompletionStage<RespRequestHandler> sendSubscriptions(ChannelHandlerContext ctx, CompletionStage<Void> stageToWaitFor,
         Collection<byte[]> keyChannels, boolean subscribeOrUnsubscribe) {
      return stageToReturn(stageToWaitFor, ctx, (__, alloc) -> {
         assert ctx.executor().inEventLoop();

         Long counter = ctx.channel().attr(SUBSCRIPTIONS_COUNTER).get();
         if (counter == null) counter = 0L;

         for (byte[] keyChannel : keyChannels) {
            String initialCharSeq = subscribeOrUnsubscribe ? "*3\r\n$9\r\nsubscribe\r\n$" : "*3\r\n$11\r\nunsubscribe\r\n$";
            counter = Math.max(0, counter + (subscribeOrUnsubscribe ? 1 : -1));

            // Length of string (all ascii so 1 byte per) + (log10 + 1 = sizes of number as char in bytes) + \r\n + bytes themselves + \r\n + : + countSize + \r\n
            int countSize = counter == 0 ? 1 : (int) Math.log10(counter) + 1;
            int sizeRequired = initialCharSeq.length() + (int) Math.log10(keyChannel.length) + 1 + 2 + keyChannel.length + 2 + 1 + countSize + 2;
            ByteBuf subscribeBuffer = alloc.apply(sizeRequired);
            int initialPos = subscribeBuffer.writerIndex();
            subscribeBuffer.writeCharSequence(initialCharSeq + keyChannel.length + CRLF_STRING, CharsetUtil.US_ASCII);
            subscribeBuffer.writeBytes(keyChannel);
            subscribeBuffer.writeBytes(CRLF);
            subscribeBuffer.writeByte(':');
            ByteBufferUtils.setIntChars(counter, countSize, subscribeBuffer);
            subscribeBuffer.writeBytes(CRLF);
            assert subscribeBuffer.writerIndex() - initialPos == sizeRequired;
         }

         if (counter == 0) {
            ctx.channel().attr(SUBSCRIPTIONS_COUNTER).set(null);
         } else {
            ctx.channel().attr(SUBSCRIPTIONS_COUNTER).set(counter);
         }
      });
   }
}
