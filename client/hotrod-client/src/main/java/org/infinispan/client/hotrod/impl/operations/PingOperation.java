package org.infinispan.client.hotrod.impl.operations;

import java.util.concurrent.atomic.AtomicInteger;

import net.jcip.annotations.Immutable;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.exceptions.InvalidResponseException;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HeaderParams;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.jboss.logging.BasicLogger;

/**
 * Corresponds to the "ping" operation as defined in <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod protocol specification</a>.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Immutable
public class PingOperation extends HotRodOperation {

   private static final BasicLogger log = LogFactory.getLog(PingOperation.class);
   private static final boolean trace = log.isTraceEnabled();

   private final Transport transport;

   public PingOperation(Codec codec, AtomicInteger topologyId, Configuration cfg, Transport transport) {
      this(codec, topologyId, cfg, transport, DEFAULT_CACHE_NAME_BYTES);
   }

   public PingOperation(Codec codec, AtomicInteger topologyId, Configuration cfg, Transport transport, byte[] cacheName) {
      super(codec, 0, cfg, cacheName, topologyId);
      this.transport = transport;
   }

   @Override
   public PingResult execute() {
      try {
         HeaderParams params = writeHeader(transport, HotRodConstants.PING_REQUEST);
         transport.flush();

         short respStatus = readHeaderAndValidate(transport, params);
         if (HotRodConstants.isSuccess(respStatus)) {
            if (trace)
               log.tracef("Successfully validated transport: %s", transport);
            return HotRodConstants.hasCompatibility(respStatus)
               ? PingResult.SUCCESS_WITH_COMPAT
               : PingResult.SUCCESS;
         } else {
            String hexStatus = Integer.toHexString(respStatus);
            if (trace)
               log.tracef("Unknown response status: %s", hexStatus);

            throw new InvalidResponseException(
                  "Unexpected response status: " + hexStatus);
         }
      } catch (HotRodClientException e) {
         if (e.getMessage().contains("CacheNotFoundException"))
            return PingResult.CACHE_DOES_NOT_EXIST;

         // Any other situation, rethrow the exception
         throw e;
      }
   }

   public static enum PingResult {
      // Success if the ping request was responded correctly
      SUCCESS,
      // Success with compatibility enabled
      SUCCESS_WITH_COMPAT,
      // When the ping request fails due to non-existing cache
      CACHE_DOES_NOT_EXIST,
      // For any other type of failures
      FAIL;

      public boolean isSuccess() {
         return this == SUCCESS || this == SUCCESS_WITH_COMPAT;
      }

      public boolean hasCompatibility() {
         return this == SUCCESS_WITH_COMPAT;
      }
   }
}
