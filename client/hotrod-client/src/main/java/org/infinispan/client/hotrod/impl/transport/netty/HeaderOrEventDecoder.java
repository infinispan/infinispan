package org.infinispan.client.hotrod.impl.transport.netty;

import java.net.SocketAddress;
import java.util.List;
import java.util.function.Consumer;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.event.ClientEvent;
import org.infinispan.client.hotrod.impl.operations.AddClientListenerOperation;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HeaderParams;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.util.Either;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

public class HeaderOrEventDecoder extends HintedReplayingDecoder<Void> {
   private static final Log log = LogFactory.getLog(HeaderOrEventDecoder.class);

   private final Codec codec;
   private final HeaderParams params;
   private final ChannelFactory channelFactory;
   private final AddClientListenerOperation operation;
   private final Consumer<ClientEvent> eventConsumer;
   private final byte[] listenerId;
   private final Configuration configuration;

   public HeaderOrEventDecoder(Codec codec, HeaderParams params, ChannelFactory channelFactory,
                               AddClientListenerOperation operation, Consumer<ClientEvent> eventConsumer, byte[] listenerId, Configuration configuration) {
      this.codec = codec;
      this.params = params;
      this.channelFactory = channelFactory;
      this.operation = operation;
      this.eventConsumer = eventConsumer;
      this.listenerId = listenerId;
      this.configuration = configuration;
   }

   @Override
   protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
      // TODO ISPN-8621
      // The only operation using this decoder is AddClientListenerOperation and that one does not release the channel
      codec.readMessageId(in);
      Either<Short, ClientEvent> either = codec.readHeaderOrEvent(in, params, listenerId, channelFactory.getMarshaller(),
            configuration.serialWhitelist(), channelFactory, ctx.channel().remoteAddress());
      switch (either.type()) {
         case LEFT:
            operation.acceptResponse(in, either.left(), null);
            break;
         case RIGHT:
            operation.postponeTimeout(ctx.channel());
            eventConsumer.accept(either.right());
            break;
      }
   }

   @Override
   public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      operation.exceptionCaught(ctx, cause);
   }

   @Override
   public void channelInactive(ChannelHandlerContext ctx) throws Exception {
      SocketAddress address = ctx.channel().remoteAddress();
      operation.exceptionCaught(ctx, log.connectionClosed(address, address));
   }
}
