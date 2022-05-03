package org.infinispan.hotrod.impl.operations;

import org.infinispan.api.common.CacheOptions;
import org.infinispan.hotrod.impl.cache.ServerStatistics;
import org.infinispan.hotrod.impl.cache.ServerStatisticsImpl;
import org.infinispan.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * Implements to the stats operation as defined by <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod
 * protocol specification</a>.
 *
 * @since 14.0
 */
public class StatsOperation extends RetryOnFailureOperation<ServerStatistics> {
   private ServerStatisticsImpl result;
   private int numStats = -1;

   public StatsOperation(OperationContext operationContext, CacheOptions options) {
      super(operationContext, STATS_REQUEST, STATS_RESPONSE, options, null);
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
