package org.infinispan.client.hotrod.impl.operations;

import net.jcip.annotations.Immutable;
import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
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

   private static final Log log = LogFactory.getLog(PingOperation.class);

   private final Transport transport;

   public PingOperation(AtomicInteger topologyId, Transport transport) {
      this(topologyId, transport, DEFAULT_CACHE_NAME_BYTES);
   }

   public PingOperation(AtomicInteger topologyId, Transport transport, byte[] cacheName) {
      super(null, cacheName, topologyId);
      this.transport = transport;
   }

   @Override
   public PingResult execute() {
      try {
         long messageId = writeHeader(transport, HotRodConstants.PING_REQUEST);
         short respStatus = readHeaderAndValidate(transport, messageId, HotRodConstants.PING_RESPONSE);
         if (respStatus == HotRodConstants.NO_ERROR_STATUS) {
            if (log.isTraceEnabled())
               log.trace("Successfully validated transport: " + transport);
            return PingResult.SUCCESS;
         } else {
            if (log.isTraceEnabled())
               log.trace("Unknown response status: " + respStatus);
            return PingResult.FAIL;
         }
      } catch (HotRodClientException e) {
         if (e.getMessage().contains("CacheNotFoundException"))
            return PingResult.CACHE_DOES_NOT_EXIST;
         else
            return PingResult.FAIL;
      } catch (Exception e) {
         if (log.isTraceEnabled())
            log.trace("Failed to validate transport: " + transport, e);
         return PingResult.FAIL;
      }
   }

   public static enum PingResult {
      // Success if the ping request was responded correctly
      SUCCESS,
      // When the ping request fails due to non-existing cache
      CACHE_DOES_NOT_EXIST,
      // For any other type of failures
      FAIL,
   }
}
