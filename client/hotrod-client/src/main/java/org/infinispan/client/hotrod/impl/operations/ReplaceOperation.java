package org.infinispan.client.hotrod.impl.operations;

import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.impl.ClientStatistics;
import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;

import com.google.errorprone.annotations.Immutable;

import io.netty.buffer.ByteBuf;

/**
 * Implements "Replace" operation as defined by  <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod
 * protocol specification</a>.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Immutable
public class ReplaceOperation<V> extends AbstractKeyValueOperation<V> {

   public ReplaceOperation(InternalRemoteCache<?, ?> cache, byte[] keyBytes, byte[] value,
                           long lifespan, TimeUnit lifespanTimeUnit, long maxIdle, TimeUnit maxIdleTimeUnit) {
      super(cache, keyBytes, value, lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit);
   }

   @Override
   public V createResponse(ByteBuf buf, short status, HeaderDecoder decoder, Codec codec, CacheUnmarshaller unmarshaller) {
      return returnPossiblePrevValue(buf, status, codec, unmarshaller);
   }

   @Override
   public void handleStatsCompletion(ClientStatistics statistics, long startTime, short status, Object responseValue) {
      if (HotRodConstants.isNotExist(status)) {
         statistics.dataStore(startTime, 1);
      }
      if (HotRodConstants.hasPrevious(status)) {
         statistics.dataRead(true, startTime, 1);
      }
   }

   @Override
   public short requestOpCode() {
      return REPLACE_REQUEST;
   }

   @Override
   public short responseOpCode() {
      return REPLACE_RESPONSE;
   }
}
