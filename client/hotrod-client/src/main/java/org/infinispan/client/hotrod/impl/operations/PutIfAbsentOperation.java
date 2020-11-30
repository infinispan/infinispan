package org.infinispan.client.hotrod.impl.operations;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.ClientStatistics;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.jboss.logging.BasicLogger;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import net.jcip.annotations.Immutable;

/**
 * Implements "putIfAbsent" operation as described in  <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod
 * protocol specification</a>.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Immutable
public class PutIfAbsentOperation<V> extends AbstractKeyValueOperation<V> {

   private static final BasicLogger log = LogFactory.getLog(PutIfAbsentOperation.class);

   public PutIfAbsentOperation(Codec codec, ChannelFactory channelFactory,
                               Object key, byte[] keyBytes, byte[] cacheName, AtomicInteger topologyId,
                               int flags, Configuration cfg, byte[] value, long lifespan,
                               TimeUnit lifespanTimeUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit,
                               DataFormat dataFormat, ClientStatistics clientStatistics) {
      super(PUT_IF_ABSENT_REQUEST, PUT_IF_ABSENT_RESPONSE, codec, channelFactory, key, keyBytes, cacheName, topologyId, flags, cfg, value,
            lifespan, lifespanTimeUnit, maxIdleTime, maxIdleTimeUnit, dataFormat, clientStatistics);
   }

   @Override
   protected void executeOperation(Channel channel) {
      scheduleRead(channel);
      sendKeyValueOperation(channel);
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      if (HotRodConstants.isNotExecuted(status)) {
         V prevValue = returnPossiblePrevValue(buf, status);
         if (HotRodConstants.hasPrevious(status)) {
            statsDataRead(true);
         }
         if (log.isTraceEnabled()) {
            log.tracef("Returning from putIfAbsent: %s", prevValue);
         }
         complete(prevValue);
      } else {
         statsDataStore();
         complete(null);
      }
   }
}
