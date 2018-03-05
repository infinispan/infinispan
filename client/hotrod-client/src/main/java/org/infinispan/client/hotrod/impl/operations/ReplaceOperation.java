package org.infinispan.client.hotrod.impl.operations;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import net.jcip.annotations.Immutable;

/**
 * Implements "Replace" operation as defined by  <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod
 * protocol specification</a>.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Immutable
public class ReplaceOperation<V> extends AbstractKeyValueOperation<V> {

   public ReplaceOperation(Codec codec, ChannelFactory channelFactory,
                           Object key, byte[] keyBytes, byte[] cacheName, AtomicInteger topologyId,
                           int flags, Configuration cfg, byte[] value,
                           long lifespan, TimeUnit lifespanTimeUnit, long maxIdle, TimeUnit maxIdleTimeUnit) {
      super(REPLACE_REQUEST, REPLACE_RESPONSE, codec, channelFactory, key, keyBytes, cacheName, topologyId, flags, cfg, value,
            lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit);
   }

   @Override
   protected void executeOperation(Channel channel) {
      scheduleRead(channel);
      sendKeyValueOperation(channel);
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      complete(returnPossiblePrevValue(buf, status));
   }
}
