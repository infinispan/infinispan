package org.infinispan.client.hotrod.impl.operations;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.infinispan.client.hotrod.impl.ClientStatistics;
import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * Implements "getAll" as defined by  <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod protocol specification</a>.
 *
 * @author William Burns
 * @since 7.2
 */
public class GetAllOperation<K, V> extends AbstractCacheOperation<Map<K, V>> {

   private Map<K, V> result;
   private int size = -1;

   public GetAllOperation(InternalRemoteCache<?, ?> remoteCache, Set<byte[]> keys) {
      super(remoteCache);
      this.keys = keys;
   }

   protected final Set<byte[]> keys;

   @Override
   public void writeOperationRequest(Channel channel, ByteBuf buf, Codec codec) {
      ByteBufUtil.writeVInt(buf, keys.size());
      for (byte[] key : keys) {
         ByteBufUtil.writeArray(buf, key);
      }
   }

   @Override
   public void reset() {
      size = -1;
      result = null;
   }

   @Override
   public Map<K, V> createResponse(ByteBuf buf, short status, HeaderDecoder decoder, Codec codec, CacheUnmarshaller unmarshaller) {
      if (size < 0) {
         size = ByteBufUtil.readVInt(buf);
         result = new HashMap<>(size);
         decoder.checkpoint();
      }
      while (result.size() < size) {
         K key = unmarshaller.readKey(buf);
         V value = unmarshaller.readValue(buf);
         result.put(key, value);
         decoder.checkpoint();
      }
      return result;
   }

   @Override
   void handleStatsCompletion(ClientStatistics statistics, long startTime, short status, Map<K, V> responseValue) {
      statistics.dataRead(true, startTime, responseValue.size());
      statistics.dataRead(false, startTime, keys.size() - responseValue.size());
   }

   @Override
   public short requestOpCode() {
      return GET_ALL_REQUEST;
   }

   @Override
   public short responseOpCode() {
      return GET_ALL_RESPONSE;
   }
}
