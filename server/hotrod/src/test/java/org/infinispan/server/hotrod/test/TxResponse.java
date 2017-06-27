package org.infinispan.server.hotrod.test;

import org.infinispan.server.hotrod.HotRodOperation;
import org.infinispan.server.hotrod.OperationStatus;

/**
 * A {@link TestResponse} used by prepare, commit or rollback requests.
 *
 * @author Pedro Ruivo
 * @since 9.1
 */
public class TxResponse extends TestResponse {

   public final int xaCode;

   TxResponse(byte version, long messageId, String cacheName, short clientIntel,
         HotRodOperation operation, OperationStatus status,
         int topologyId, AbstractTestTopologyAwareResponse topologyResponse, int xaCode) {
      super(version, messageId, cacheName, clientIntel, operation, status, topologyId, topologyResponse);
      this.xaCode = xaCode;
   }
}
