package org.infinispan.server.hotrod.test;

import org.infinispan.server.hotrod.OperationResponse;
import org.infinispan.server.hotrod.OperationStatus;

/**
 * @author wburns
 * @since 9.0
 */
public class TestCustomEvent extends TestResponse {
   public final byte[] listenerId;
   public final boolean isRetried;
   public final byte[] eventData;

   protected TestCustomEvent(byte version, long messageId, String cacheName, OperationResponse operation, byte[] listenerId, boolean isRetried, byte[] eventData) {
      super(version, messageId, cacheName, (byte) 0, operation, OperationStatus.Success, (byte) 0, null);
      this.listenerId = listenerId;
      this.isRetried = isRetried;
      this.eventData = eventData;
   }
}
