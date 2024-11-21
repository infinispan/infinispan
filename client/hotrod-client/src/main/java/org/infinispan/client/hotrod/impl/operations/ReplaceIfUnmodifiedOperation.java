package org.infinispan.client.hotrod.impl.operations;

import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.impl.ClientStatistics;
import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.VersionedOperationResponse;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * Implement "replaceIfUnmodified" as defined by  <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod
 * protocol specification</a>.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class ReplaceIfUnmodifiedOperation<V> extends AbstractKeyValueOperation<VersionedOperationResponse<V>> {
   private final long version;

   public ReplaceIfUnmodifiedOperation(InternalRemoteCache<?, ?> remoteCache, byte[] keyBytes, byte[] value,
                                       long lifespan, TimeUnit lifespanTimeUnit, long maxIdle, TimeUnit maxIdleTimeUnit,
                                       long version) {
      super(remoteCache, keyBytes, value, lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit);
      this.version = version;
   }

   @Override
   public void writeOperationRequest(Channel channel, ByteBuf buf, Codec codec) {
      ByteBufUtil.writeArray(buf, keyBytes);
      codec.writeExpirationParams(buf, lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit);
      buf.writeLong(version);
      ByteBufUtil.writeArray(buf, valueBytes);
   }

   @Override
   public VersionedOperationResponse<V> createResponse(ByteBuf buf, short status, HeaderDecoder decoder, Codec codec, CacheUnmarshaller unmarshaller) {
      return returnVersionedOperationResponse(buf, status, codec, unmarshaller);
   }

   @Override
   public void handleStatsCompletion(ClientStatistics statistics, long startTime, short status, VersionedOperationResponse<V> responseValue) {
      if (HotRodConstants.isSuccess(status)) {
         statistics.dataStore(startTime, 1);
      }
   }

   @Override
   public short requestOpCode() {
      return REPLACE_IF_UNMODIFIED_REQUEST;
   }

   @Override
   public short responseOpCode() {
      return REPLACE_IF_UNMODIFIED_RESPONSE;
   }
}
