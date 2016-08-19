package org.infinispan.server.hotrod.test;

import org.infinispan.server.hotrod.OperationResponse;
import org.infinispan.server.hotrod.OperationStatus;

/**
 * @author wburns
 * @since 9.0
 */
public class TestKeyWithVersionEvent extends TestResponse {
   public final byte[] listenerId;
   public final boolean isRetried;
   public final byte[] key;
   public final long dataVersion;

   protected TestKeyWithVersionEvent(byte version, long messageId, String cacheName, OperationResponse operation,
                                     byte[] listenerId, boolean isRetried, byte[] key, long dataVersion) {
      super(version, messageId, cacheName, (byte) 0, operation, OperationStatus.Success, (byte) 0, null);
      this.listenerId = listenerId;
      this.isRetried = isRetried;
      this.key = key;
      this.dataVersion = dataVersion;
   }
}
