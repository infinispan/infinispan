package org.infinispan.client.hotrod.impl.transport.netty;

import java.util.List;
import java.util.function.Consumer;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.event.ClientEvent;
import org.infinispan.client.hotrod.impl.operations.HotRodOperation;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HeaderParams;
import org.infinispan.commons.util.Either;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

public class HeaderOrEventDecoder<T> extends HintedReplayingDecoder<Void> {
   private final Codec codec;
   private final HeaderParams params;
   private final ChannelFactory channelFactory;
   private final HotRodOperation<T> operation;
   private final Consumer<ClientEvent> eventConsumer;
   private final byte[] listenerId;
   private final Configuration configuration;

   public HeaderOrEventDecoder(Codec codec, HeaderParams params, ChannelFactory channelFactory,
                               HotRodOperation<T> operation, Consumer<ClientEvent> eventConsumer, byte[] listenerId, Configuration configuration) {
      this.codec = codec;
      this.params = params;
      this.channelFactory = channelFactory;
      this.operation = operation;
      this.eventConsumer = eventConsumer;
      this.listenerId = listenerId;
      this.configuration = configuration;
   }

   @Override
   protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
      Either<Short, ClientEvent> either = codec.readHeaderOrEvent(in, params, listenerId, channelFactory.getMarshaller(),
            configuration.serialWhitelist(), channelFactory, ctx.channel().remoteAddress());
      switch (either.type()) {
         case LEFT:
            T result = operation.decodePayload(in, either.left());
            try {
               ctx.pipeline().remove(this);
               ctx.pipeline().remove(operation);
               operation.releaseChannel(ctx.channel());
            } catch (Throwable t) {
               try {
                  ctx.channel().close();
               } catch (Throwable t2) {
                  t.addSuppressed(t2);
               }
               operation.completeExceptionally(t);
               return;
            }
            operation.complete(result);
            break;
         case RIGHT:
            eventConsumer.accept(either.right());
            break;
      }
   }
}
