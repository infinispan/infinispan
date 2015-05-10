package org.infinispan.client.hotrod.impl.protocol;

import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;

/**
 * A Hot Rod encoder/decoder for version 1.2 of the protocol.
 *
 * @author Tristan Tarrant
 * @author Galder Zamarreño
 * @since 5.2
 */
public class Codec12 extends Codec11 {

   private static final Log log = LogFactory.getLog(Codec12.class, Log.class);

   @Override
   public HeaderParams writeHeader(Transport transport, HeaderParams params) {
      return writeHeader(transport, params, HotRodConstants.VERSION_12);
   }

   @Override
   protected HeaderParams writeHeader(Transport transport, HeaderParams params, byte version) {
      transport.writeByte(HotRodConstants.REQUEST_MAGIC);
      transport.writeVLong(params.messageId(MSG_ID.incrementAndGet()).messageId);
      transport.writeByte(version);
      transport.writeByte(params.opCode);
      transport.writeArray(params.cacheName);

      int joinedFlags = HeaderParams.joinFlags(params.flags);
      transport.writeVInt(joinedFlags);
      transport.writeByte(params.clientIntel);
      transport.writeVInt(params.topologyId.get());
      //todo change once TX support is added
      transport.writeByte(params.txMarker);
      getLog().tracef("Wrote header for message %d. Operation code: %#04x. Flags: %#x",
            params.messageId, params.opCode, joinedFlags);
      return params;
   }

   @Override
   public Log getLog() {
      return log;
   }

}
