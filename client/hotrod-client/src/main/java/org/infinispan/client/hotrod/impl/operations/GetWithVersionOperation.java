package org.infinispan.client.hotrod.impl.operations;

import static org.infinispan.client.hotrod.marshall.MarshallerUtil.bytes2obj;

import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.VersionedValue;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.ClientStatistics;
import org.infinispan.client.hotrod.impl.VersionedValueImpl;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import net.jcip.annotations.Immutable;

/**
 * Corresponds to getWithVersion operation as described by
 * <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod protocol specification</a>.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Immutable
@Deprecated
public class GetWithVersionOperation<V> extends AbstractKeyOperation<VersionedValue<V>> {

   private static final Log log = LogFactory.getLog(GetWithVersionOperation.class);
   private static final boolean trace = log.isTraceEnabled();

   public GetWithVersionOperation(Codec codec, ChannelFactory channelFactory, Object key, byte[] keyBytes,
                                  byte[] cacheName, AtomicInteger topologyId, int flags,
                                  Configuration cfg, DataFormat dataFormat, ClientStatistics clientStatistics) {
      super(GET_WITH_VERSION, GET_WITH_VERSION_RESPONSE, codec, channelFactory, key, keyBytes, cacheName, topologyId,
            flags, cfg, dataFormat, clientStatistics);
   }

   @Override
   protected void executeOperation(Channel channel) {
      scheduleRead(channel);
      sendArrayOperation(channel, keyBytes);
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      if (HotRodConstants.isNotExist(status) || !HotRodConstants.isSuccess(status)) {
         statsDataRead(false);
         complete(null);
         return;
      }
      long version = ByteBufUtil.readVLong(buf);
      if (trace) {
         log.tracef("Received version: %d", version);
      }
      V value = bytes2obj(channelFactory.getMarshaller(), ByteBufUtil.readArray(buf), dataFormat.isObjectStorage(), cfg.getClassWhiteList());
      statsDataRead(true);
      complete(new VersionedValueImpl<V>(version, value));
   }
}
