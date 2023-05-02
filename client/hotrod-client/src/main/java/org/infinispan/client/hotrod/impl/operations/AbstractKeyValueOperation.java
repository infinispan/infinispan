package org.infinispan.client.hotrod.impl.operations;

import static org.infinispan.commons.util.Util.printArray;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.ClientStatistics;
import org.infinispan.client.hotrod.impl.ClientTopology;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.telemetry.impl.TelemetryService;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import net.jcip.annotations.Immutable;

/**
 * Base class for all operations that manipulate a key and a value.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Immutable
public abstract class AbstractKeyValueOperation<T> extends AbstractKeyOperation<T> {

   protected final byte[] value;

   protected final long lifespan;

   protected final long maxIdle;

   protected final TimeUnit lifespanTimeUnit;

   protected final TimeUnit maxIdleTimeUnit;

   protected AbstractKeyValueOperation(short requestCode, short responseCode, Codec codec, ChannelFactory channelFactory, Object key, byte[] keyBytes, byte[] cacheName,
                                       AtomicReference<ClientTopology> clientTopology, int flags, Configuration cfg, byte[] value,
                                       long lifespan, TimeUnit lifespanTimeUnit, long maxIdle, TimeUnit maxIdleTimeUnit,
                                       DataFormat dataFormat, ClientStatistics clientStatistics, TelemetryService telemetryService) {
      super(requestCode, responseCode, codec, channelFactory, key, keyBytes, cacheName, clientTopology, flags, cfg, dataFormat, clientStatistics, telemetryService);
      this.value = value;
      this.lifespan = lifespan;
      this.maxIdle = maxIdle;
      this.lifespanTimeUnit = lifespanTimeUnit;
      this.maxIdleTimeUnit = maxIdleTimeUnit;
   }

   protected void sendKeyValueOperation(Channel channel) {
      ByteBuf buf = channel.alloc().buffer(codec.estimateHeaderSize(header) + keyBytes.length +
            codec.estimateExpirationSize(lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit) + value.length);

      codec.writeHeader(buf, header);
      ByteBufUtil.writeArray(buf, keyBytes);
      codec.writeExpirationParams(buf, lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit);
      ByteBufUtil.writeArray(buf, value);
      channel.writeAndFlush(buf);
   }

   @Override
   public void writeBytes(Channel channel, ByteBuf buf) {
      writeArrayOperation(buf, keyBytes);
      codec.writeExpirationParams(buf, lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit);
      ByteBufUtil.writeArray(buf, value);
   }

   @Override
   protected void addParams(StringBuilder sb) {
      super.addParams(sb);
      sb.append(", value=").append(printArray(value));
   }
}
