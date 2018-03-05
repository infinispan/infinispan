package org.infinispan.client.hotrod.impl.operations;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import net.jcip.annotations.Immutable;

/**
 * Performs a step in the challenge/response authentication operation
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
@Immutable
public class AuthOperation extends HotRodOperation<byte[]> {
   private static final Log log = LogFactory.getLog(AuthOperation.class);

   private final Channel channel;
   private final String saslMechanism;
   private final byte[] response;

   public AuthOperation(Codec codec, AtomicInteger topologyId, Configuration cfg, Channel channel,
                        ChannelFactory channelFactory, String saslMechanism, byte[] response) {
      super(AUTH_REQUEST, AUTH_RESPONSE, codec, 0,  cfg, DEFAULT_CACHE_NAME_BYTES, topologyId, channelFactory);
      this.channel = channel;
      this.saslMechanism = saslMechanism;
      this.response = response;
   }

   @Override
   public CompletableFuture<byte[]> execute() {
      if (!channel.isActive()) {
         throw log.channelInactive(channel.remoteAddress(), channel.remoteAddress());
      }

      byte[] saslMechBytes = saslMechanism.getBytes(HOTROD_STRING_CHARSET);

      scheduleRead(channel);

      ByteBuf buf = channel.alloc().buffer(codec.estimateHeaderSize(header) +
            ByteBufUtil.estimateArraySize(saslMechBytes) + ByteBufUtil.estimateArraySize(response));

      codec.writeHeader(buf, header);
      ByteBufUtil.writeArray(buf, saslMechBytes);
      ByteBufUtil.writeArray(buf, response);
      channel.writeAndFlush(buf);

      return this;
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      boolean complete = buf.readUnsignedByte() > 0;
      byte challenge[] = ByteBufUtil.readArray(buf);
      complete(complete ? null : challenge);
   }
}
