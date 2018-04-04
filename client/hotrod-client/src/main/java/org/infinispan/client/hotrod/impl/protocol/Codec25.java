package org.infinispan.client.hotrod.impl.protocol;

import org.infinispan.client.hotrod.impl.transport.Transport;

/**
 * @author gustavonalle
 * @since 8.2
 */
public class Codec25 extends Codec24 {

   @Override
   public HeaderParams writeHeader(Transport transport, HeaderParams params) {
      return writeHeader(transport, params, HotRodConstants.VERSION_25);
   }

   @Override
   public short readMeta(Transport transport) {
      return transport.readByte();
   }
}
