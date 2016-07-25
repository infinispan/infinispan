package org.infinispan.server.hotrod.test;

import static org.infinispan.server.hotrod.OperationStatus.Success;

import java.util.Map;

import org.infinispan.server.hotrod.OperationResponse;

/**
 * @author wburns
 * @since 9.0
 */
public class TestStatsResponse extends TestResponse {
   final Map<String, String> stats;

   protected TestStatsResponse(byte version, long messageId, String cacheName, short clientIntel,
                               int topologyId, AbstractTestTopologyAwareResponse topologyResponse, Map<String, String> stats) {
      super(version, messageId, cacheName, clientIntel, OperationResponse.StatsResponse, Success, topologyId, topologyResponse);
      this.stats = stats;
   }
}
