package org.infinispan.client.hotrod.impl.operations;

import java.util.concurrent.atomic.AtomicInteger;

import net.jcip.annotations.Immutable;

import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HeaderParams;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.Transport;

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

   protected final Flag[] flags;

   public final byte[] cacheName;

   protected final AtomicInteger topologyId;

   protected final Codec codec;

   private static final byte NO_TX = 0;
   private static final byte XA_TX = 1;

   protected HotRodOperation(Codec codec, Flag[] flags, byte[] cacheName, AtomicInteger topologyId) {
      this.flags = flags;
      this.cacheName = cacheName;
      this.topologyId = topologyId;
      this.codec = codec;
   }

   public abstract Object execute();

   protected final HeaderParams writeHeader(Transport transport, short operationCode) {
      HeaderParams params = new HeaderParams()
            .opCode(operationCode).cacheName(cacheName).flags(flags)
            .clientIntel(CLIENT_INTELLIGENCE_HASH_DISTRIBUTION_AWARE)
            .topologyId(topologyId).txMarker(NO_TX);
      return codec.writeHeader(transport, params);
   }

   /**
    * Magic	| Message Id | Op code | Status | Topology Change Marker
    */
   protected short readHeaderAndValidate(Transport transport, HeaderParams params) {
      return codec.readHeader(transport, params);
   }

}
