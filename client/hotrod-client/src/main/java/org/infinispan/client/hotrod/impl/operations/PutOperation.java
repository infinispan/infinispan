package org.infinispan.client.hotrod.impl.operations;

import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.exceptions.InvalidResponseException;
import org.infinispan.client.hotrod.impl.ClientStatistics;
import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;

import com.google.errorprone.annotations.Immutable;

import io.netty.buffer.ByteBuf;

/**
 * Implements "put" as defined by  <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod protocol specification</a>.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Immutable
public class PutOperation<V> extends AbstractKeyValueOperation<MetadataValue<V>> {
   public PutOperation(InternalRemoteCache<?, ?> cache, byte[] keyBytes, byte[] valueBytes, long lifespan,
                       TimeUnit lifespanTimeUnit, long maxIdle, TimeUnit maxIdleTimeUnit) {
      super(cache, keyBytes, valueBytes, lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit);
   }

   @Override
   public MetadataValue<V> createResponse(ByteBuf buf, short status, HeaderDecoder decoder, Codec codec, CacheUnmarshaller unmarshaller) {
      if (HotRodConstants.isSuccess(status)) {
         return returnMetadataValue(buf, status, codec, unmarshaller);
      } else {
         throw new InvalidResponseException("Unexpected response status: " + Integer.toHexString(status));
      }
   }

   @Override
   public void handleStatsCompletion(ClientStatistics statistics, long startTime, short status, MetadataValue<V> responseValue) {
      statistics.dataStore(startTime, 1);
      if (HotRodConstants.hasPrevious(status)) {
         statistics.dataRead(true, startTime, 1);
      }
   }

   @Override
   public short requestOpCode() {
      return PUT_REQUEST;
   }

   @Override
   public short responseOpCode() {
      return PUT_RESPONSE;
   }
}
