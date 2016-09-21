package org.infinispan.client.hotrod.impl.operations;

import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.configuration.ClientIntelligence;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HeaderParams;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.Transport;

import net.jcip.annotations.Immutable;

/**
 * Generic Hot Rod operation. It is aware of {@link org.infinispan.client.hotrod.Flag}s and it is targeted against a
 * cache name. This base class encapsulates the knowledge of writing and reading a header, as described in the
 * <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod protocol specification</a>
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Immutable
public abstract class HotRodOperation implements HotRodConstants {

   protected final int flags;

   public final byte[] cacheName;

   protected final AtomicInteger topologyId;

   protected final Codec codec;

   protected final ClientIntelligence clientIntelligence;

   private static final byte NO_TX = 0;
   private static final byte XA_TX = 1;

   protected HotRodOperation(Codec codec, int flags, ClientIntelligence clientIntelligence, byte[] cacheName, AtomicInteger topologyId) {
      this.flags = flags;
      this.clientIntelligence = clientIntelligence;
      this.cacheName = cacheName;
      this.topologyId = topologyId;
      this.codec = codec;
   }

   public abstract Object execute();

   protected final HeaderParams writeHeader(Transport transport, short operationCode) {
      HeaderParams params = new HeaderParams()
            .opCode(operationCode).cacheName(cacheName).flags(flags)
            .clientIntel(clientIntelligence)
            .topologyId(topologyId).txMarker(NO_TX)
            .topologyAge(transport.getTransportFactory().getTopologyAge());
      return codec.writeHeader(transport, params);
   }

   /**
    * Magic | Message Id | Op code | Status | Topology Change Marker
    */
   protected short readHeaderAndValidate(Transport transport, HeaderParams params) {
      return codec.readHeader(transport, params);
   }

}
