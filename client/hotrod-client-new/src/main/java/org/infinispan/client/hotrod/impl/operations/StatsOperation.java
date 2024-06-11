package org.infinispan.client.hotrod.impl.operations;

import org.infinispan.client.hotrod.ServerStatistics;
import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.ServerStatisticsImpl;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;

/**
 * Implements to the stats operation as defined by <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod protocol specification</a>.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class StatsOperation extends AbstractCacheOperation<ServerStatistics> {
   private ServerStatisticsImpl result;
   private int numStats = -1;

   public StatsOperation(InternalRemoteCache<?, ?> remoteCache) {
      super(remoteCache);
   }

   @Override
   public ServerStatistics createResponse(ByteBuf buf, short status, HeaderDecoder decoder, Codec codec, CacheUnmarshaller unmarshaller) {
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
      return result;
   }

   @Override
   public short requestOpCode() {
      return STATS_REQUEST;
   }

   @Override
   public short responseOpCode() {
      return STATS_RESPONSE;
   }

   @Override
   public void reset() {
      numStats = -1;
      result = null;
   }
}
