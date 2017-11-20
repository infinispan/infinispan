package org.infinispan.server.hotrod.counter.response;

import org.infinispan.server.hotrod.HotRodOperation;
import org.infinispan.server.hotrod.OperationStatus;
import org.infinispan.server.hotrod.test.AbstractTestTopologyAwareResponse;
import org.infinispan.server.hotrod.test.TestResponse;

/**
 * A counter test response for operations that return the counter's value.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public class CounterValueTestResponse extends TestResponse {

   private final long value;

   public CounterValueTestResponse(byte version, long messageId, String cacheName, short clientIntel,
         HotRodOperation operation, OperationStatus status, int topologyId,
         AbstractTestTopologyAwareResponse topologyResponse, long value) {
      super(version, messageId, cacheName, clientIntel, operation, status, topologyId, topologyResponse);
      this.value = value;
   }

   public long getValue() {
      return value;
   }
}
