package org.infinispan.server.hotrod.test;

import org.infinispan.server.hotrod.OperationResponse;
import org.infinispan.server.hotrod.OperationStatus;

/**
 * @author wburns
 * @since 9.0
 */
public class TestKeyEvent extends TestResponse {
   public final byte[] listenerId;
   public final boolean isRetried;
   public final byte[] key;

   protected TestKeyEvent(byte version, long messageId, String cacheName, byte[] listenerId, boolean isRetried, byte[] key) {
      super(version, messageId, cacheName, (byte) 0, OperationResponse.CacheEntryRemovedEventResponse,
            OperationStatus.Success, (byte) 0, null);
      this.listenerId = listenerId;
      this.isRetried = isRetried;
      this.key = key;
   }
}
