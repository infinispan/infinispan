package org.infinispan.server.hotrod.counter.response;

import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.server.hotrod.HotRodOperation;
import org.infinispan.server.hotrod.OperationStatus;
import org.infinispan.server.hotrod.test.AbstractTestTopologyAwareResponse;
import org.infinispan.server.hotrod.test.TestResponse;

/**
 * A {@link TestResponse} extension that contains the {@link CounterConfiguration}.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public class CounterConfigurationTestResponse extends TestResponse {

   private final CounterConfiguration configuration;

   public CounterConfigurationTestResponse(byte version, long messageId, String cacheName, short clientIntel,
         HotRodOperation operation, OperationStatus status, int topologyId,
         AbstractTestTopologyAwareResponse topologyResponse, CounterConfiguration configuration) {
      super(version, messageId, cacheName, clientIntel, operation, status, topologyId, topologyResponse);
      this.configuration = configuration;
   }

   public CounterConfiguration getConfiguration() {
      return configuration;
   }
}
