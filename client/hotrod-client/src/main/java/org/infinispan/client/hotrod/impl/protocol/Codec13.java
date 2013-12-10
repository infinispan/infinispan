package org.infinispan.client.hotrod.impl.protocol;

import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;

/**
 * A Hot Rod encoder/decoder for version 1.3 of the protocol.
 *
 * @author Adrian Nistor
 * @since 6.1
 */
public class Codec13 extends Codec12 {

   private static final Log log = LogFactory.getLog(Codec13.class, Log.class);

   @Override
   public HeaderParams writeHeader(Transport transport, HeaderParams params) {
      return writeHeader(transport, params, HotRodConstants.VERSION_13);
   }

   @Override
   public Log getLog() {
      return log;
   }

}
