package org.infinispan.client.hotrod.impl.operations;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * Obtains a list of SASL authentication mechanisms supported by the server
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class AuthMechListOperation extends HotRodOperation<List<String>> {
   private static final Log log = LogFactory.getLog(AuthMechListOperation.class);
   private final Channel channel;
   private int mechCount = -1;
   private List<String> result;

   public AuthMechListOperation(Codec codec, AtomicInteger topologyId, Configuration cfg, Channel channel, ChannelFactory channelFactory) {
      super(AUTH_MECH_LIST_REQUEST, AUTH_MECH_LIST_RESPONSE, codec, 0, cfg, DEFAULT_CACHE_NAME_BYTES, topologyId, channelFactory);
      this.channel = channel;
   }

   @Override
   public CompletableFuture<List<String>> execute() {
      if (!channel.isActive()) {
         throw log.channelInactive(channel.remoteAddress(), channel.remoteAddress());
      }
      scheduleRead(channel);
      sendHeader(channel);
      return this;
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      if (mechCount < 0) {
         mechCount = ByteBufUtil.readVInt(buf);
         result = new ArrayList<>(mechCount);
         decoder.checkpoint();
      }
      while (result.size() < mechCount) {
         result.add(ByteBufUtil.readString(buf));
         decoder.checkpoint();
      }
      complete(result);
   }
}
