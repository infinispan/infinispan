package org.infinispan.client.hotrod.impl.multimap.operations;

import static org.infinispan.client.hotrod.impl.multimap.protocol.MultimapHotRodConstants.CONTAINS_VALUE_MULTIMAP_REQUEST;
import static org.infinispan.client.hotrod.impl.multimap.protocol.MultimapHotRodConstants.CONTAINS_VALUE_MULTIMAP_RESPONSE;

import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.operations.AbstractCacheOperation;
import org.infinispan.client.hotrod.impl.operations.CacheUnmarshaller;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
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
public class ContainsValueMultimapOperation extends AbstractCacheOperation<Boolean> {

   protected final byte[] value;
   private final long lifespan;
   private final long maxIdle;
   private final TimeUnit lifespanTimeUnit;
   private final TimeUnit maxIdleTimeUnit;
   private final boolean supportsDuplicates;

   protected ContainsValueMultimapOperation(InternalRemoteCache<?, ?> remoteCache, byte[] value,
                                            long lifespan, TimeUnit lifespanTimeUnit, long maxIdle,
                                            TimeUnit maxIdleTimeUnit, boolean supportsDuplicates) {
      super(remoteCache);
      this.value = value;
      this.lifespan = lifespan;
      this.maxIdle = maxIdle;
      this.lifespanTimeUnit = lifespanTimeUnit;
      this.maxIdleTimeUnit = maxIdleTimeUnit;
      this.supportsDuplicates = supportsDuplicates;
   }

   @Override
   public void writeOperationRequest(Channel channel, ByteBuf buf, Codec codec) {
      codec.writeExpirationParams(buf, lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit);
      ByteBufUtil.writeArray(buf, value);
      codec.writeMultimapSupportDuplicates(buf, supportsDuplicates);
   }

   @Override
   public Boolean createResponse(ByteBuf buf, short status, HeaderDecoder decoder, Codec codec, CacheUnmarshaller unmarshaller) {
      if (HotRodConstants.isNotExist(status)) {
         return Boolean.FALSE;
      } else {
         return buf.readByte() == 1 ? Boolean.TRUE : Boolean.FALSE;
      }
   }

   @Override
   public short requestOpCode() {
      return CONTAINS_VALUE_MULTIMAP_REQUEST;
   }

   @Override
   public short responseOpCode() {
      return CONTAINS_VALUE_MULTIMAP_RESPONSE;
   }
}
