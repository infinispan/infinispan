package org.infinispan.client.hotrod.impl.operations;

import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

public abstract class AbstractKeyValueOperation<V> extends AbstractKeyOperation<V> {
   protected final byte[] valueBytes;
   protected final long lifespan;

   protected final long maxIdle;

   protected final TimeUnit lifespanTimeUnit;

   protected final TimeUnit maxIdleTimeUnit;
   protected AbstractKeyValueOperation(InternalRemoteCache<?, ?> internalRemoteCache, byte[] keyBytes, byte[] valueBytes,
                                       long lifespan, TimeUnit lifespanTimeUnit, long maxIdle, TimeUnit maxIdleTimeUnit) {
      super(internalRemoteCache, keyBytes);

      this.valueBytes = valueBytes;
      this.lifespan = lifespan;
      this.maxIdle = maxIdle;
      this.lifespanTimeUnit = lifespanTimeUnit;
      this.maxIdleTimeUnit = maxIdleTimeUnit;
   }

   @Override
   public void writeOperationRequest(Channel channel, ByteBuf buf, Codec codec) {
      super.writeOperationRequest(channel, buf, codec);
      codec.writeExpirationParams(buf, lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit);
      ByteBufUtil.writeArray(buf, valueBytes);
   }
}
