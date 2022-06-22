package org.infinispan.client.hotrod.impl.multimap.operations;

import static org.infinispan.client.hotrod.impl.multimap.protocol.MultimapHotRodConstants.PUT_MULTIMAP_REQUEST;
import static org.infinispan.client.hotrod.impl.multimap.protocol.MultimapHotRodConstants.PUT_MULTIMAP_RESPONSE;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.exceptions.InvalidResponseException;
import org.infinispan.client.hotrod.impl.ClientStatistics;
import org.infinispan.client.hotrod.impl.ClientTopology;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import net.jcip.annotations.Immutable;

/**
 * Implements "put" for multimap cache as defined by  <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod
 * protocol specification</a>.
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.2
 */
@Immutable
public class PutKeyValueMultimapOperation extends AbstractMultimapKeyValueOperation<Void> {

   public PutKeyValueMultimapOperation(Codec codec,
                                       ChannelFactory channelFactory,
                                       Object key, byte[] keyBytes, byte[] cacheName, AtomicReference<ClientTopology> clientTopology,
                                       int flags, Configuration cfg, byte[] value, long lifespan,
                                       TimeUnit lifespanTimeUnit, long maxIdle, TimeUnit maxIdleTimeUnit, DataFormat dataFormat,
                                       ClientStatistics clientStatistics, boolean supportsDuplicates) {
      super(PUT_MULTIMAP_REQUEST, PUT_MULTIMAP_RESPONSE, codec, channelFactory, key, keyBytes, cacheName, clientTopology,
            flags, cfg, value, lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit, dataFormat, clientStatistics, supportsDuplicates);
   }

   @Override
   protected void executeOperation(Channel channel) {
      scheduleRead(channel);
      sendKeyValueOperation(channel);
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      if (!HotRodConstants.isSuccess(status)) {
         throw new InvalidResponseException("Unexpected response status: " + Integer.toHexString(status));
      }
      complete(null);
   }
}
