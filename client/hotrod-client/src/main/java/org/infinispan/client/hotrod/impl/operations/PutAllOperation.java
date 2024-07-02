package org.infinispan.client.hotrod.impl.operations;

import java.net.SocketAddress;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.exceptions.InvalidResponseException;
import org.infinispan.client.hotrod.impl.ClientStatistics;
import org.infinispan.client.hotrod.impl.ClientTopology;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;
import org.infinispan.client.hotrod.telemetry.impl.TelemetryService;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import net.jcip.annotations.Immutable;

/**
 * Implements "putAll" as defined by  <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod protocol specification</a>.
 *
 * @author William Burns
 * @since 7.2
 */
@Immutable
public class PutAllOperation extends StatsAffectingRetryingOperation<Void> {

   public PutAllOperation(Codec codec, ChannelFactory channelFactory,
                          Map<byte[], byte[]> map, byte[] cacheName, AtomicReference<ClientTopology> clientTopology,
                          int flags, Configuration cfg,
                          long lifespan, TimeUnit lifespanTimeUnit, long maxIdle, TimeUnit maxIdleTimeUnit,
                          DataFormat dataFormat, ClientStatistics clientStatistics, TelemetryService telemetryService) {
      super(PUT_ALL_REQUEST, PUT_ALL_RESPONSE, codec, channelFactory, cacheName, clientTopology, flags, cfg, dataFormat,
            clientStatistics, telemetryService);
      this.map = map;
      this.lifespan = lifespan;
      this.lifespanTimeUnit = lifespanTimeUnit;
      this.maxIdle = maxIdle;
      this.maxIdleTimeUnit = maxIdleTimeUnit;
   }

   protected final Map<byte[], byte[]> map;
   protected final long lifespan;
   private final TimeUnit lifespanTimeUnit;
   protected final long maxIdle;
   private final TimeUnit maxIdleTimeUnit;

   @Override
   protected void executeOperation(Channel channel) {
      scheduleRead(channel);

      int bufSize = codec.estimateHeaderSize(header) + ByteBufUtil.estimateVIntSize(map.size()) +
            codec.estimateExpirationSize(lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit);
      for (Entry<byte[], byte[]> entry : map.entrySet()) {
         bufSize += ByteBufUtil.estimateArraySize(entry.getKey());
         bufSize += ByteBufUtil.estimateArraySize(entry.getValue());
      }
      ByteBuf buf = channel.alloc().buffer(bufSize);

      codec.writeHeader(buf, header);
      codec.writeExpirationParams(buf, lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit);
      ByteBufUtil.writeVInt(buf, map.size());
      for (Entry<byte[], byte[]> entry : map.entrySet()) {
         ByteBufUtil.writeArray(buf, entry.getKey());
         ByteBufUtil.writeArray(buf, entry.getValue());
      }
      channel.writeAndFlush(buf);
   }

   @Override
   public void writeBytes(Channel channel, ByteBuf buf) {

      codec.writeHeader(buf, header);
      codec.writeExpirationParams(buf, lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit);
      ByteBufUtil.writeVInt(buf, map.size());
      for (Entry<byte[], byte[]> entry : map.entrySet()) {
         ByteBufUtil.writeArray(buf, entry.getKey());
         ByteBufUtil.writeArray(buf, entry.getValue());
      }
   }

   @Override
   protected void fetchChannelAndInvoke(int retryCount, Set<SocketAddress> failedServers) {
      channelFactory.fetchChannelAndInvoke(map.keySet().iterator().next(), failedServers, cacheName(), this);
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      if (HotRodConstants.isSuccess(status)) {
         statsDataStore(map.size());
         complete(null);
         return;
      }
      throw new InvalidResponseException("Unexpected response status: " + Integer.toHexString(status));
   }
}
