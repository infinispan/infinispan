package org.infinispan.client.hotrod.impl.multimap.operations;

import static org.infinispan.client.hotrod.impl.multimap.protocol.MultimapHotRodConstants.CONTAINS_VALUE_MULTIMAP_REQUEST;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.multimap.protocol.MultimapHeaderParams;
import org.infinispan.client.hotrod.impl.operations.RetryOnFailureOperation;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HeaderParams;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;

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

   protected ContainsValueMultimapOperation(Codec codec, ChannelFactory channelFactory, byte[] cacheName,
                                            AtomicInteger topologyId, int flags, Configuration cfg, byte[] value,
                                            long lifespan, TimeUnit lifespanTimeUnit, long maxIdle, TimeUnit maxIdleTimeUnit) {
      super(codec, channelFactory, cacheName, topologyId, flags, cfg);
      this.value = value;
      this.lifespan = lifespan;
      this.maxIdle = maxIdle;
      this.lifespanTimeUnit = lifespanTimeUnit;
      this.maxIdleTimeUnit = maxIdleTimeUnit;
   }

   @Override
   protected HeaderParams createHeader() {
      return new MultimapHeaderParams();
   }

   @Override
   protected void executeOperation(Channel channel) {
      HeaderParams header = headerParams(CONTAINS_VALUE_MULTIMAP_REQUEST);
      scheduleRead(channel, header);
      sendValueOperation(channel, header);
   }

   @Override
   public Boolean decodePayload(ByteBuf buf, short status) {
      if (HotRodConstants.isNotExist(status)) {
         return Boolean.FALSE;
      }

      return buf.readByte() == 1 ? Boolean.TRUE : Boolean.FALSE;
   }

   protected void sendValueOperation(Channel channel, HeaderParams header) {
      ByteBuf buf = channel.alloc().buffer(codec.estimateHeaderSize(header) +
            codec.estimateExpirationSize(lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit) +
            ByteBufUtil.estimateArraySize(value));
      codec.writeHeader(buf, header);
      codec.writeExpirationParams(buf, lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit);
      ByteBufUtil.writeArray(buf, value);
      channel.writeAndFlush(buf);
   }
}
