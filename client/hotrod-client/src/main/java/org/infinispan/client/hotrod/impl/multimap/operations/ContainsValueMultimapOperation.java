package org.infinispan.client.hotrod.impl.multimap.operations;

import static org.infinispan.client.hotrod.impl.multimap.protocol.MultimapHotRodConstants.CONTAINS_VALUE_MULTIMAP_REQUEST;
import static org.infinispan.client.hotrod.impl.multimap.protocol.MultimapHotRodConstants.CONTAINS_VALUE_MULTIMAP_RESPONSE;

import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.multimap.protocol.MultimapHeaderParams;
import org.infinispan.client.hotrod.impl.operations.RetryOnFailureOperation;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HeaderParams;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;

/**
 * Implements "contains value" for multimap cache as defined by  <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot
 * Rod protocol specification</a>.
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.2
 */
public class ContainsValueMultimapOperation extends RetryOnFailureOperation<Boolean> {

   protected final byte[] value;
   private final long lifespan;
   private final long maxIdle;
   private final TimeUnit lifespanTimeUnit;
   private final TimeUnit maxIdleTimeUnit;

   protected ContainsValueMultimapOperation(Codec codec, TransportFactory transportFactory, byte[] cacheName,
                                            AtomicInteger topologyId, int flags, Configuration cfg, byte[] value,
                                            long lifespan, TimeUnit lifespanTimeUnit, long maxIdle, TimeUnit maxIdleTimeUnit) {
      super(codec, transportFactory, cacheName, topologyId, flags, cfg);
      this.value = value;
      this.lifespan = lifespan;
      this.maxIdle = maxIdle;
      this.lifespanTimeUnit = lifespanTimeUnit;
      this.maxIdleTimeUnit = maxIdleTimeUnit;
   }

   @Override
   protected HeaderParams createHeader() {
      return new MultimapHeaderParams();
   }

   @Override
   protected Transport getTransport(int retryCount, Set failedServers) {
      return transportFactory.getTransport(failedServers, cacheName);
   }

   @Override
   protected Boolean executeOperation(Transport transport) {
      short status = sendValueOperation(transport, CONTAINS_VALUE_MULTIMAP_REQUEST, CONTAINS_VALUE_MULTIMAP_RESPONSE);
      if (HotRodConstants.isNotExist(status)) {
         return Boolean.FALSE;
      }

      return transport.readByte() == 1 ? Boolean.TRUE : Boolean.FALSE;
   }

   //[header][key][lifespan][max idle][value length][value]
   protected short sendValueOperation(Transport transport, short opCode, short opRespCode) {
      // 1) write header
      HeaderParams params = writeHeader(transport, opCode);

      // 2) write value
      codec.writeExpirationParams(transport, lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit);
      transport.writeArray(value);
      transport.flush();

      // 3) now read header
      return readHeaderAndValidate(transport, params);
   }
}
