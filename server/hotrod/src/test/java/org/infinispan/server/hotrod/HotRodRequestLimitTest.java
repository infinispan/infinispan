package org.infinispan.server.hotrod;

import java.nio.channels.ClosedChannelException;
import java.util.concurrent.CompletionException;

import org.infinispan.commons.test.Exceptions;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.server.hotrod.test.HotRodTestingUtil;
import org.infinispan.server.hotrod.test.RemoteTransaction;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * HotRod test to ensure requests larger than configured limits are not processed
 *
 * @author William Burns
 * @since 15.1
 */
@Test(groups = "functional", testName = "server.hotrod.HotRodRequestLimitTest")
public class HotRodRequestLimitTest extends HotRodSingleNodeTest {
   private static final int MAX_BYTE_ARRAY_SIZE = 128;
   private static final int MAX_KEY_COUNT = 10;

   @AfterMethod
   public void restartClient() {
      hotRodClient = connectClient();
   }

   protected HotRodServer createStartHotRodServer(EmbeddedCacheManager cacheManager) {
      HotRodServerConfigurationBuilder builder = new HotRodServerConfigurationBuilder()
            .maxByteArraySize(MAX_BYTE_ARRAY_SIZE)
            .maxKeyCount(MAX_KEY_COUNT);
      return HotRodTestingUtil.startHotRodServer(cacheManager, builder);
   }

   public void testKeyTooLong() {
      byte[] key = new byte[MAX_BYTE_ARRAY_SIZE + 2];
      Exceptions.expectException(CompletionException.class, ClosedChannelException.class,
            () -> client().put(key, -1, -1, new byte[]{1, 3, 4}));
   }

   public void testValueTooLong() {
      byte[] value = new byte[MAX_BYTE_ARRAY_SIZE + 2];
      Exceptions.expectException(CompletionException.class, ClosedChannelException.class,
            () -> client().put(new byte[]{1, 3, 4}, -1, -1, value));
   }

   public void testLongCacheName() {
      Exceptions.expectException(CompletionException.class, ClosedChannelException.class,
            () -> client().execute(0xA0, (byte) 0x01, "1".repeat(MAX_BYTE_ARRAY_SIZE + 1),
                  new byte[]{23}, -1, -1, new byte[]{23}, 0, (byte) 1, 0));
   }

   public void testTooManyKeysInTx() {
      RemoteTransaction tx = RemoteTransaction.startTransaction(client());
      for (byte i = 0; i < (byte) MAX_KEY_COUNT + 1; ++i) {
         tx.set(new byte[] { i }, new byte[] { i });
      }

      Exceptions.expectException(CompletionException.class, ClosedChannelException.class,
            () -> tx.prepare());
   }
}
