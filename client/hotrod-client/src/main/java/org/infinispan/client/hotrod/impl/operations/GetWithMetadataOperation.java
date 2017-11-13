package org.infinispan.client.hotrod.impl.operations;

import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.MetadataValueImpl;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HeaderParams;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import net.jcip.annotations.Immutable;

/**
 * Corresponds to getWithMetadata operation as described by
 * <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod protocol specification</a>.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
@Immutable
public class GetWithMetadataOperation<V> extends AbstractKeyOperation<MetadataValue<V>> {

   private static final Log log = LogFactory.getLog(GetWithMetadataOperation.class);
   private static final boolean trace = log.isTraceEnabled();

   public GetWithMetadataOperation(Codec codec, ChannelFactory channelFactory, Object key, byte[] keyBytes,
                                   byte[] cacheName, AtomicInteger topologyId, int flags,
                                   Configuration cfg) {
      super(codec, channelFactory, key, keyBytes, cacheName, topologyId, flags, cfg);
   }

   @Override
   protected void executeOperation(Channel channel) {
      HeaderParams header = headerParams(GET_WITH_METADATA);
      scheduleRead(channel, header);
      sendArrayOperation(channel, header, keyBytes);
   }

   @Override
   public MetadataValue<V> decodePayload(ByteBuf buf, short status) {
      if (HotRodConstants.isNotExist(status) || !HotRodConstants.isSuccess(status)) {
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
      if (trace) {
         log.tracef("Received version: %d", version);
      }
      V value = codec.readUnmarshallByteArray(buf, status, cfg.serialWhitelist(), channelFactory.getMarshaller());
      return new MetadataValueImpl<V>(creation, lifespan, lastUsed, maxIdle, version, value);
   }
}
