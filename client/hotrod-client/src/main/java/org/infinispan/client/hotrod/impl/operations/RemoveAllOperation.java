package org.infinispan.client.hotrod.impl.operations;

import java.util.Set;

import org.infinispan.client.hotrod.exceptions.InvalidResponseException;
import org.infinispan.client.hotrod.impl.ClientStatistics;
import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

public class RemoveAllOperation extends AbstractCacheOperation<Void> {
   protected final Set<byte[]> keys;

   public RemoveAllOperation(InternalRemoteCache<?, ?> remoteCache, Set<byte[]> keys) {
      super(remoteCache);
      this.keys = keys;
   }

   @Override
   public void writeOperationRequest(Channel channel, ByteBuf buf, Codec codec) {
      ByteBufUtil.writeVInt(buf, keys.size());
      for (byte[] key : keys) {
         ByteBufUtil.writeArray(buf, key);
      }
   }

   @Override
   public Void createResponse(ByteBuf buf, short status, HeaderDecoder decoder, Codec codec, CacheUnmarshaller unmarshaller) {
      if (HotRodConstants.isSuccess(status)) {
         return null;
      }
      throw new InvalidResponseException("Unexpected response status: " + Integer.toHexString(status));
   }

   @Override
   public void handleStatsCompletion(ClientStatistics statistics, long startTime, short status, Void responseValue) {
      statistics.dataRemove(startTime, keys.size());
   }

   @Override
   public short requestOpCode() {
      return REMOVE_ALL_REQUEST;
   }

   @Override
   public short responseOpCode() {
      return REMOVE_ALL_RESPONSE;
   }
}
