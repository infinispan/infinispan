package org.infinispan.client.hotrod.impl.operations;

import java.util.concurrent.atomic.AtomicReference;

import org.infinispan.client.hotrod.ServerStatistics;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.ClientTopology;
import org.infinispan.client.hotrod.impl.ServerStatisticsImpl;
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
public class StatsOperation extends RetryOnFailureOperation<ServerStatistics> {
   private ServerStatisticsImpl result;
   private int numStats = -1;

   public StatsOperation(Codec codec, ChannelFactory channelFactory,
                         byte[] cacheName, AtomicReference<ClientTopology> clientTopology, int flags, Configuration cfg) {
      super(STATS_REQUEST, STATS_RESPONSE, codec, channelFactory, cacheName, clientTopology, flags, cfg, null, null);
   }

   @Override
   protected void executeOperation(Channel channel) {
      sendHeaderAndRead(channel);
   }

   @Override
   public void writeBytes(Channel channel, ByteBuf buf) {
      codec.writeHeader(buf, header);
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
         result = new ServerStatisticsImpl();
         decoder.checkpoint();
      }
      while (result.size() < numStats) {
         String statName = ByteBufUtil.readString(buf);
         String statValue = ByteBufUtil.readString(buf);
         result.addStats(statName, statValue);
         decoder.checkpoint();
      }
      complete(result);
   }
}
