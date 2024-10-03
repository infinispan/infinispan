package org.infinispan.client.hotrod.impl.operations;

import org.infinispan.client.hotrod.impl.ClientStatistics;
import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import net.jcip.annotations.Immutable;

/**
 * Implement "remove" operation as described in <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod
 * protocol specification</a>.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Immutable
public class RemoveOperation<V> extends AbstractKeyOperation<V> {

   public RemoveOperation(InternalRemoteCache<?, ?> cache, byte[] keyBytes) {
      super(cache, keyBytes);
   }

   @Override
   public V createResponse(ByteBuf buf, short status, HeaderDecoder decoder, Codec codec, CacheUnmarshaller unmarshaller) {
      V result = returnPossiblePrevValue(buf, status, codec, unmarshaller);
      if (HotRodConstants.isNotExist(status)) {
         return null;
      }
      return result;
   }

   @Override
   public void handleStatsCompletion(ClientStatistics statistics, long startTime, short status, V responseValue) {
      statistics.dataRemove(startTime, 1);
   }

   @Override
   public short requestOpCode() {
      return REMOVE_REQUEST;
   }

   @Override
   public short responseOpCode() {
      return REMOVE_RESPONSE;
   }
}
