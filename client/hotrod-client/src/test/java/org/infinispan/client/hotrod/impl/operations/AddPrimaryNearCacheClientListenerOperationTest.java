package org.infinispan.client.hotrod.impl.operations;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.BitSet;

import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.Codec42;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.testng.annotations.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * Unit tests verifying serialization/framing of protocol 4.2 operations.
 */
@Test(groups = "unit", testName = "client.hotrod.impl.operations.AddPrimaryNearCacheClientListenerOperationTest")
public class AddPrimaryNearCacheClientListenerOperationTest {

   private InternalRemoteCache<?, ?> mockCache() {
      InternalRemoteCache<?, ?> cache = mock(InternalRemoteCache.class);
      when(cache.getName()).thenReturn("test-cache");
      when(cache.getNameBytes()).thenReturn(new byte[]{'t', 'e', 's', 't'});
      return cache;
   }

   public void testOperationOpCodes() {
      byte[] listenerId = new byte[]{1, 2, 3, 4};
      AddPrimaryNearCacheClientListenerOperation op =
            new AddPrimaryNearCacheClientListenerOperation(mockCache(), new Object(), listenerId, 1024);

      assertThat(op.requestOpCode()).isEqualTo(HotRodConstants.ADD_PRIMARY_NEAR_CACHE_LISTENER_REQUEST);
      assertThat(op.responseOpCode()).isEqualTo(HotRodConstants.ADD_PRIMARY_NEAR_CACHE_LISTENER_RESPONSE);
      assertThat(op.getBloomFilterBits()).isEqualTo(1024);
      assertThat(op.supportRetry()).isFalse();
   }

   public void testWriteOperationRequest() {
      byte[] listenerId = new byte[]{10, 20, 30, 40};
      int bloomBits = 2048;
      AddPrimaryNearCacheClientListenerOperation op =
            new AddPrimaryNearCacheClientListenerOperation(mockCache(), new Object(), listenerId, bloomBits);

      Codec codec = new Codec42(new BitSet());
      ByteBuf buf = Unpooled.buffer();
      try {
         op.writeOperationRequest(null, buf, codec);

         byte[] readListenerId = ByteBufUtil.readArray(buf);
         assertThat(readListenerId).isEqualTo(listenerId);

         int readBloomBits = ByteBufUtil.readVInt(buf);
         assertThat(readBloomBits).isEqualTo(bloomBits);
         assertThat(buf.isReadable()).isFalse();
      } finally {
         buf.release();
      }
   }

   public void testCodec42Capabilities() {
      Codec42 codec42 = new Codec42(new BitSet());
      assertThat(codec42.isPrimaryNearCacheListenerSupported()).isTrue();
   }
}
