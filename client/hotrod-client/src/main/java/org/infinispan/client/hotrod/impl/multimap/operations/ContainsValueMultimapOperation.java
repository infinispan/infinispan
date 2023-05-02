package org.infinispan.client.hotrod.impl.multimap.operations;

import static org.infinispan.client.hotrod.impl.multimap.protocol.MultimapHotRodConstants.CONTAINS_VALUE_MULTIMAP_REQUEST;
import static org.infinispan.client.hotrod.impl.multimap.protocol.MultimapHotRodConstants.CONTAINS_VALUE_MULTIMAP_RESPONSE;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.ClientTopology;
import org.infinispan.client.hotrod.impl.operations.RetryOnFailureOperation;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * Implements "contains value" for multimap cache as defined by  <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot
 * Rod protocol specification</a>.
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.2
 */
public class ContainsValueMultimapOperation extends RetryOnFailureOperation<Boolean> {

   protected final byte[] value;
   private final long lifespan;
   private final long maxIdle;
   private final TimeUnit lifespanTimeUnit;
   private final TimeUnit maxIdleTimeUnit;
   private final boolean supportsDuplicates;

   protected ContainsValueMultimapOperation(Codec codec, ChannelFactory channelFactory, byte[] cacheName,
                                            AtomicReference<ClientTopology> clientTopology, int flags, Configuration cfg, byte[] value,
                                            long lifespan, TimeUnit lifespanTimeUnit, long maxIdle, TimeUnit maxIdleTimeUnit, boolean supportsDuplicates) {
      super(CONTAINS_VALUE_MULTIMAP_REQUEST, CONTAINS_VALUE_MULTIMAP_RESPONSE, codec, channelFactory, cacheName,
            clientTopology, flags, cfg, null, null);
      this.value = value;
      this.lifespan = lifespan;
      this.maxIdle = maxIdle;
      this.lifespanTimeUnit = lifespanTimeUnit;
      this.maxIdleTimeUnit = maxIdleTimeUnit;
      this.supportsDuplicates = supportsDuplicates;
   }

   @Override
   protected void executeOperation(Channel channel) {
      scheduleRead(channel);
      sendValueOperation(channel);
   }

   @Override
   public void writeBytes(Channel channel, ByteBuf buf) {
      codec.writeHeader(buf, header);
      codec.writeExpirationParams(buf, lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit);
      ByteBufUtil.writeArray(buf, value);
      codec.writeMultimapSupportDuplicates(buf, supportsDuplicates);
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      if (HotRodConstants.isNotExist(status)) {
         complete(Boolean.FALSE);
      } else {
         complete(buf.readByte() == 1 ? Boolean.TRUE : Boolean.FALSE);
      }
   }

   protected void sendValueOperation(Channel channel) {
      ByteBuf buf = channel.alloc().buffer(codec.estimateHeaderSize(header) +
            codec.estimateExpirationSize(lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit) +
            ByteBufUtil.estimateArraySize(value) +
            codec.estimateSizeMultimapSupportsDuplicated());
      codec.writeHeader(buf, header);
      codec.writeExpirationParams(buf, lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit);
      ByteBufUtil.writeArray(buf, value);
      codec.writeMultimapSupportDuplicates(buf, supportsDuplicates);
      channel.writeAndFlush(buf);
   }
}
