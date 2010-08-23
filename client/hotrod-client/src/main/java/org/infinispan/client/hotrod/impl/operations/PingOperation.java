package org.infinispan.client.hotrod.impl.operations;

import net.jcip.annotations.Immutable;
import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Corresponds to the "ping" operation as defined in <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod protocol specification</a>.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Immutable
public class PingOperation extends HotRodOperation {

   private static Log log = LogFactory.getLog(PingOperation.class);

   private final Transport transport;

   public PingOperation(Flag[] flags, AtomicInteger topologyId, Transport transport) {
      super(flags, DEFAULT_CACHE_NAME_BYTES, topologyId);
      this.transport = transport;
   }

   @Override
   public Object execute() {
      boolean success;
      try {
         long messageId = writeHeader(transport, HotRodConstants.PING_REQUEST);
         short respStatus = readHeaderAndValidate(transport, messageId, HotRodConstants.PING_RESPONSE);
         if (respStatus == HotRodConstants.NO_ERROR_STATUS) {
            if (log.isTraceEnabled())
               log.trace("Successfully validated transport: " + transport);
            success = true;
         } else {
            if (log.isTraceEnabled())
               log.trace("Unknown response status: " + respStatus);
            success = false;
         }
      } catch (Exception e) {
         if (log.isTraceEnabled())
            log.trace("Failed to validate transport: " + transport, e);
         success = false;
      }
      return success;
   }
}
