package org.infinispan.client.hotrod.impl.operations;

import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.impl.ClientStatistics;
import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.MetadataValueImpl;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * Corresponds to getWithMetadata operation as described by
 * <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod protocol specification</a>.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class GetWithMetadataOperation<V> extends AbstractKeyOperation<GetWithMetadataOperation.GetWithMetadataResult<V>> {

   public record GetWithMetadataResult<V>(MetadataValue<V> value, boolean retried) {};

   private static final Log log = LogFactory.getLog(GetWithMetadataOperation.class);

   private final Channel preferredChannel;

   public GetWithMetadataOperation(InternalRemoteCache<?, ?> remoteCache, byte[] keyBytes, Channel preferredChannel) {
      super(remoteCache, keyBytes);
      // We should always be passing resolved addresses here to confirm it matches
      this.preferredChannel = preferredChannel;
   }

   @Override
   public GetWithMetadataResult<V> createResponse(ByteBuf buf, short status, HeaderDecoder decoder, Codec codec, CacheUnmarshaller unmarshaller) {
      boolean retried = preferredChannel != null && !preferredChannel.equals(decoder.getChannel());
      MetadataValue<V> metadataValue = readMetadataValue(buf, status, unmarshaller);
      return new GetWithMetadataResult<>(metadataValue, retried);
   }

   @Override
   public void handleStatsCompletion(ClientStatistics statistics, long startTime, short status,
                              GetWithMetadataOperation.GetWithMetadataResult<V> responseValue) {
      statistics.dataRead(responseValue.value != null, startTime, 1);
   }

   public static <V> MetadataValue<V> readMetadataValue(ByteBuf buf, short status, CacheUnmarshaller unmarshaller) {
      if (HotRodConstants.isNotExist(status) || (!HotRodConstants.isSuccess(status) && !HotRodConstants.hasPrevious(status))) {
         return null;
      }
      short flags = buf.readUnsignedByte();
      long creation = -1;
      int lifespan = -1;
      long lastUsed = -1;
      int maxIdle = -1;
      if ((flags & INFINITE_LIFESPAN) != INFINITE_LIFESPAN) {
         creation = buf.readLong();
         lifespan = ByteBufUtil.readVInt(buf);
      }
      if ((flags & INFINITE_MAXIDLE) != INFINITE_MAXIDLE) {
         lastUsed = buf.readLong();
         maxIdle = ByteBufUtil.readVInt(buf);
      }
      long version = buf.readLong();
      if (log.isTraceEnabled()) {
         log.tracef("Received version: %d", version);
      }
      V value = unmarshaller.readValue(buf);

      return new MetadataValueImpl<>(creation, lifespan, lastUsed, maxIdle, version, value);
   }

   @Override
   public short requestOpCode() {
      return GET_WITH_METADATA;
   }

   @Override
   public short responseOpCode() {
      return GET_WITH_METADATA_RESPONSE;
   }
}
