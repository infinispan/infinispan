package org.infinispan.client.hotrod.impl.operations;

import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.impl.ClientStatistics;
import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.jboss.logging.BasicLogger;

import io.netty.buffer.ByteBuf;

/**
 * Implements "putIfAbsent" operation as described in  <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod
 * protocol specification</a>.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class PutIfAbsentOperation<V> extends AbstractKeyValueOperation<V> {

   private static final BasicLogger log = LogFactory.getLog(PutIfAbsentOperation.class);

   public PutIfAbsentOperation(InternalRemoteCache<?, ?> remoteCache, byte[] keyBytes, byte[] value, long lifespan,
                               TimeUnit lifespanTimeUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      super(remoteCache, keyBytes, value, lifespan, lifespanTimeUnit, maxIdleTime, maxIdleTimeUnit);
   }

   @Override
   public V createResponse(ByteBuf buf, short status, HeaderDecoder decoder, Codec codec, CacheUnmarshaller unmarshaller) {
      if (HotRodConstants.isNotExecuted(status)) {
         V prevValue = returnPossiblePrevValue(buf, status, codec, unmarshaller);
         if (log.isTraceEnabled()) {
            log.tracef("Returning from putIfAbsent: %s", prevValue);
         }
         return prevValue;
      }
      return null;
   }

   @Override
   void handleStatsCompletion(ClientStatistics statistics, long startTime, short status, Object responseValue) {
      if (HotRodConstants.isNotExecuted(status)) {
         if (HotRodConstants.hasPrevious(status)) {
            statistics.dataRead(true, startTime, 1);
         }
      } else {
         statistics.dataStore(startTime, 1);
      }
   }

   @Override
   public short requestOpCode() {
      return PUT_IF_ABSENT_REQUEST;
   }

   @Override
   public short responseOpCode() {
      return PUT_IF_ABSENT_RESPONSE;
   }
}
