package org.infinispan.client.hotrod.impl.operations;

import java.net.SocketAddress;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import net.jcip.annotations.Immutable;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.exceptions.InvalidResponseException;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HeaderParams;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;

/**
 * Implements "putAll" as defined by  <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod protocol specification</a>.
 *
 * @author William Burns
 * @since 7.2
 */
@Immutable
public class PutAllOperation extends RetryOnFailureOperation<Void> {

   public PutAllOperation(Codec codec, TransportFactory transportFactory,
                          Map<byte[], byte[]> map, byte[] cacheName, AtomicInteger topologyId,
                          int flags, Configuration cfg,
                          long lifespan, TimeUnit lifespanTimeUnit, long maxIdle, TimeUnit maxIdleTimeUnit) {
      super(codec, transportFactory, cacheName, topologyId, flags, cfg);
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
   protected Void executeOperation(Transport transport) {
      HeaderParams params = writeHeader(transport, PUT_ALL_REQUEST);
      codec.writeExpirationParams(transport, lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit);
      transport.writeVInt(map.size());
      for (Entry<byte[], byte[]> entry : map.entrySet()) {
         transport.writeArray(entry.getKey());
         transport.writeArray(entry.getValue());
      }
      transport.flush();

      short status = readHeaderAndValidate(transport, params);
      if (!HotRodConstants.isSuccess(status)) {
         throw new InvalidResponseException("Unexpected response status: " + Integer.toHexString(status));
      }
      return null;
   }

   @Override
   protected Transport getTransport(int retryCount, Set<SocketAddress> failedServers) {
      return transportFactory.getTransport(map.keySet().iterator().next(), failedServers, cacheName);
   }
}
