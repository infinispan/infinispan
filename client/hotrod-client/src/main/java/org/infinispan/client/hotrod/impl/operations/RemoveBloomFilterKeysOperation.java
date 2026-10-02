package org.infinispan.client.hotrod.impl.operations;

import java.util.List;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * Tells the server which keys are no longer present in the near cache so that it can remove them from the bloom
 * filter it maintains for this client. A key must be sent as many times as it was read, so duplicates are
 * meaningful and must not be collapsed.
 *
 * @since 16.3
 */
public class RemoveBloomFilterKeysOperation extends AbstractCacheOperation<Void> {
   private final List<byte[]> keys;

   protected RemoveBloomFilterKeysOperation(InternalRemoteCache<?, ?> remoteCache, List<byte[]> keys) {
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
      return null;
   }

   @Override
   public short requestOpCode() {
      return REMOVE_BLOOM_FILTER_KEYS_REQUEST;
   }

   @Override
   public short responseOpCode() {
      return REMOVE_BLOOM_FILTER_KEYS_RESPONSE;
   }

   @Override
   public DataFormat getDataFormat() {
      // Keys are already marshalled, so no data format is needed
      return null;
   }
}
