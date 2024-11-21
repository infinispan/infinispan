package org.infinispan.client.hotrod.impl.operations;

import org.infinispan.client.hotrod.impl.ClientStatistics;
import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;

public class GetOperation<V> extends AbstractKeyOperation<V> {
   public GetOperation(InternalRemoteCache<?, ?> internalRemoteCache, byte[] keyBytes) {
      super(internalRemoteCache, keyBytes);
   }

   @Override
   public V createResponse(ByteBuf buf, short status, HeaderDecoder decoder, Codec codec, CacheUnmarshaller unmarshaller) {
      if (!HotRodConstants.isNotExist(status) && HotRodConstants.isSuccess(status)) {
         return unmarshaller.readValue(buf);
      }
      return null;
   }

   @Override
   public void handleStatsCompletion(ClientStatistics statistics, long startTime, short status, Object responseValue) {
      statistics.dataRead(!HotRodConstants.isNotExist(status) && HotRodConstants.isSuccess(status), startTime, 1);
   }

   @Override
   public short requestOpCode() {
      return HotRodConstants.GET_REQUEST;
   }

   @Override
   public short responseOpCode() {
      return HotRodConstants.GET_RESPONSE;
   }
}
