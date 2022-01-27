package org.infinispan.server.resp;

import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.server.core.logging.Log;
import org.infinispan.util.concurrent.CompletableFutures;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.CharsetUtil;

public class SubscriberHandler implements RespRequestHandler {
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass(), Log.class);
   public static final byte[] PREFIX_CHANNEL_BYTES = new byte[]{-114, 16, 78, -3, 127};
   private final Resp3Handler handler;
   private final RespServer respServer;

   public SubscriberHandler(RespServer respServer) {
      this.respServer = respServer;
      this.handler = respServer.getHandler();
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
   public RespRequestHandler handleRequest(ChannelHandlerContext ctx, String type,
                                       List<byte[]> arguments) {

      switch (type) {
         case "SUBSCRIBE":
            for (byte[] keyChannel : arguments) {
               if (log.isTraceEnabled()) {
                  log.tracef("Subscriber for channel: " + CharsetUtil.UTF_8.decode(ByteBuffer.wrap(keyChannel)));
               }
               WrappedByteArray wrappedByteArray = new WrappedByteArray(keyChannel);
               if (specificChannelSubscribers.get(wrappedByteArray) == null) {
                  PubSubListener pubSubListener = new PubSubListener(ctx.channel());
                  specificChannelSubscribers.put(wrappedByteArray, pubSubListener);
                  byte[] channel = keyToChannel(keyChannel);
                  respServer.getCache().addListenerAsync(pubSubListener, (key, prevValue, prevMetadata, value, metadata, eventType) ->
                     Arrays.equals((key), channel), null)
                        .whenComplete((ignore, t) -> {
                           if (t != null) {
                              log.warnf("There was an error adding listener for channel %s",
                                    CharsetUtil.UTF_8.decode(ByteBuffer.wrap(channel)));
                              ctx.writeAndFlush("-ERR Failure adding client listener");
                           } else {
                              ByteBuf subscribeBuffer = ctx.alloc().buffer(20 + (int) Math.log10(keyChannel.length) + 1 + keyChannel.length + 2);
                              subscribeBuffer.writeCharSequence("*2\r\n$9\r\nsubscribe\r\n$" + keyChannel.length + "\r\n", CharsetUtil.UTF_8);
                              subscribeBuffer.writeBytes(keyChannel);
                              subscribeBuffer.writeCharSequence("\r\n", CharsetUtil.UTF_8);
                              ctx.writeAndFlush(subscribeBuffer);
                           }
                        });
               }
            }
            break;
         case "UNSUBSCRIBE":
            if (arguments.size() == 0) {
               for (PubSubListener listener : specificChannelSubscribers.values()) {
                  respServer.getCache().removeListenerAsync(listener);
               }
            } else {
               for (byte[] keyChannel : arguments) {
                  WrappedByteArray wrappedByteArray = new WrappedByteArray(keyChannel);
                  PubSubListener listener = specificChannelSubscribers.remove(wrappedByteArray);
                  if (listener != null) {
                     respServer.getCache().removeListenerAsync(listener)
                           .whenComplete((ignore, t) -> {
                              if (t != null) {
                                 log.warnf("There was an error removing listener for channel %s",
                                       CharsetUtil.UTF_8.decode(ByteBuffer.wrap(keyChannel)));
                                 ctx.writeAndFlush("-ERR Failure unsubscribing client listener");
                              } else {
                                 sendUnsubscribe(ctx, keyChannel);
                              }
                           });
                  } else {
                     sendUnsubscribe(ctx, keyChannel);
                  }
               }
            }
            break;
         case "PING":
            // Note we don't return the handler and just use it to handle the ping
            handler.handleRequest(ctx, type, arguments);
            break;
         case "RESET":
            for (PubSubListener listener : specificChannelSubscribers.values()) {
               respServer.getCache().removeListenerAsync(listener);
            }
            return handler.handleRequest(ctx, type, arguments);
         case "QUIT":
            ctx.close();
            break;
         case "PSUBSCRIBE":
         case "PUNSUBSCRIBE":
            ctx.writeAndFlush(stringToByteBuf("-ERR not implemented yet" + "\r\n", ctx.alloc()));
            break;
         default:
            return RespRequestHandler.super.handleRequest(ctx, type, arguments);
      }
      return this;
   }

   private void sendUnsubscribe(ChannelHandlerContext ctx, byte[] keyChannel) {
      ByteBuf subscribeBuffer = ctx.alloc().buffer(22 + (int) Math.log10(keyChannel.length) + 1 + keyChannel.length + 2);
      subscribeBuffer.writeCharSequence("*2\r\n$11\r\nunsubscribe\r\n$" + keyChannel.length + "\r\n", CharsetUtil.UTF_8);
      subscribeBuffer.writeBytes(keyChannel);
      subscribeBuffer.writeCharSequence("\r\n", CharsetUtil.UTF_8);
      ctx.writeAndFlush(subscribeBuffer);
   }
}
