package org.infinispan.client.hotrod.impl.operations;

import java.util.concurrent.atomic.AtomicReference;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.ClientStatistics;
import org.infinispan.client.hotrod.impl.ClientTopology;
import org.infinispan.client.hotrod.impl.VersionedOperationResponse;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;
import org.infinispan.client.hotrod.telemetry.impl.TelemetryService;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import net.jcip.annotations.Immutable;

/**
 * Implements "removeIfUnmodified" operation as defined by
 * <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod protocol specification</a>.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Immutable
public class RemoveIfUnmodifiedOperation<V> extends AbstractKeyOperation<VersionedOperationResponse<V>> {

   private final long version;

   public RemoveIfUnmodifiedOperation(Codec codec, ChannelFactory channelFactory,
                                      Object key, byte[] keyBytes, byte[] cacheName, AtomicReference<ClientTopology> clientTopology,
                                      int flags, Configuration cfg,
                                      long version, DataFormat dataFormat, ClientStatistics clientStatistics,
                                      TelemetryService telemetryService) {
      super(REMOVE_IF_UNMODIFIED_REQUEST, REMOVE_IF_UNMODIFIED_RESPONSE, codec, channelFactory, key, keyBytes, cacheName,
            clientTopology, flags, cfg, dataFormat.withoutValueType(), clientStatistics, telemetryService);
      this.version = version;
   }

   @Override
   protected void executeOperation(Channel channel) {
      scheduleRead(channel);

      ByteBuf buf = channel.alloc().buffer(codec.estimateHeaderSize(header) + ByteBufUtil.estimateArraySize(keyBytes) + 8);

      codec.writeHeader(buf, header);
      ByteBufUtil.writeArray(buf, keyBytes);
      buf.writeLong(version);
      channel.writeAndFlush(buf);
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      complete(returnVersionedOperationResponse(buf, status));
   }
}
