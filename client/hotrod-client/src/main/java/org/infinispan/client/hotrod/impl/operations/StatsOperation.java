package org.infinispan.client.hotrod.impl.operations;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import net.jcip.annotations.Immutable;

/**
 * Implements to the stats operation as defined by <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod protocol specification</a>.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Immutable
public class StatsOperation extends RetryOnFailureOperation<Map<String, String>> {
   private Map<String, String> result;
   private int numStats = -1;

   public StatsOperation(Codec codec, ChannelFactory channelFactory,
                         byte[] cacheName, AtomicInteger topologyId, int flags, Configuration cfg) {
      super(STATS_REQUEST, STATS_RESPONSE, codec, channelFactory, cacheName, topologyId, flags, cfg, null);
   }

   @Override
   protected void executeOperation(Channel channel) {
      sendHeaderAndRead(channel);
   }

   @Override
   protected void reset() {
      super.reset();
      result = null;
      numStats = -1;
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      if (numStats < 0) {
         numStats = ByteBufUtil.readVInt(buf);
         result = new HashMap<>();
         decoder.checkpoint();
      }
      while (result.size() < numStats) {
         String statName = ByteBufUtil.readString(buf);
         String statValue = ByteBufUtil.readString(buf);
         result.put(statName, statValue);
         decoder.checkpoint();
      }
      complete(result);
   }
}
