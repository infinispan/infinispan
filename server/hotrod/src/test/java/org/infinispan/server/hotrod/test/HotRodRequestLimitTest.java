package org.infinispan.server.hotrod.test;

import static org.infinispan.server.hotrod.OperationStatus.Success;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.assertSuccess;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.killClient;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;

import java.nio.channels.ClosedChannelException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.HotRodSingleNodeTest;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.testing.Exceptions;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * HotRod test to ensure requests larger than configured limits are not processed
 *
 * @author William Burns
 * @since 15.2
 */
@Test(groups = "functional", testName = "server.hotrod.HotRodRequestLimitTest")
public class HotRodRequestLimitTest extends HotRodSingleNodeTest {
   private static final int MAX_CONTENT_LENGTH = 128;

   @AfterMethod
   public void restartClient() {
      killClient(hotRodClient);
      hotRodClient = connectClient();
   }

   protected HotRodServer createStartHotRodServer(EmbeddedCacheManager cacheManager) {
      HotRodServerConfigurationBuilder builder = new HotRodServerConfigurationBuilder()
            .maxContentLength(Integer.toString(MAX_CONTENT_LENGTH));
      return HotRodTestingUtil.startHotRodServer(cacheManager, builder);
   }

   public void testKeyTooLong() {
      byte[] key = new byte[MAX_CONTENT_LENGTH + 2];
      Exceptions.expectException(CompletionException.class, ClosedChannelException.class,
            () -> client().put(key, -1, -1, new byte[]{1, 3, 4}));
   }

   public void testValueTooLong() {
      byte[] value = new byte[MAX_CONTENT_LENGTH + 2];
      Exceptions.expectException(CompletionException.class, ClosedChannelException.class,
            () -> client().put(new byte[]{1, 3, 4}, -1, -1, value));
   }

   public void testLongCacheName() {
      Exceptions.expectException(CompletionException.class, ClosedChannelException.class,
            () -> client().execute(0xA0, (byte) 0x01, "1".repeat(MAX_CONTENT_LENGTH + 1),
                  new byte[]{23}, -1, -1, new byte[]{23}, 0, (byte) 1, 0));
   }

   public void testWithManyKeysTotalLarger() {
      int keySize = 4;
      RemoteTransaction tx = RemoteTransaction.startTransaction(client());
      for (byte i = 0; i < MAX_CONTENT_LENGTH / keySize; ++i) {
         tx.set(new byte[] { i, (byte) (i + 1)}, new byte[] { i, (byte) (i + 1) });
      }

      Exceptions.expectException(CompletionException.class, ClosedChannelException.class,
            () -> tx.prepare());
   }

   public void testPipelineWriteSecondOperationTooLarge() throws ExecutionException, InterruptedException, TimeoutException {
      HotRodClient client = client();

      byte[] value = new byte[MAX_CONTENT_LENGTH + 2];
      Op tooLongOp = new Op(0xA0, client.protocolVersion(), (byte) 0x01, client.defaultCacheName(),
            new byte[]{1, 2}, -1, -1, value, 0, 0, (byte) 1, 0);

      Op firstOp = new Op(0xA0, client.protocolVersion(), (byte) 0x01, client.defaultCacheName(),
            new byte[]{1, 2}, -1, -1, new byte[]{3, 4}, 0, 0, (byte) 1, 0);

      client.writeOps(firstOp, tooLongOp)
            .get(10, TimeUnit.SECONDS);

      ClientHandler handler = (ClientHandler) client.getChannel().pipeline().last();
      // Channel can be killed before we even get the response
      if (handler == null) {
         assertFalse(client.getChannel().isActive());
         return;
      }

      // The first operation should work fine
      CompletionStage<TestResponse> responseStage = handler.waitForResponse(firstOp.id);
      try {
         // It is possible for this to also throw the ClosedChannelException in some cases
         responseStage.toCompletableFuture().get(client.rspTimeoutSeconds, TimeUnit.SECONDS);

         CompletionStage<TestResponse> errorStage = handler.waitForResponse(tooLongOp.id);

         errorStage.toCompletableFuture().get(client.rspTimeoutSeconds, TimeUnit.SECONDS);
         fail("Test should have failed, but did not");
      } catch (ExecutionException e) {
         Exceptions.assertException(ExecutionException.class, ClosedChannelException.class, e);
      } catch (TimeoutException e) {
         // If it is a Timeout that is fine as well, as some machines the socket detection takes too long
      }
   }

   public void testExcessDataDoesNotCorruptSubsequentConnection() {
      byte[] oversizedValue = new byte[MAX_CONTENT_LENGTH * 4];
      for (int i = 0; i < oversizedValue.length; i++) {
         oversizedValue[i] = (byte) (i % 127 + 1);
      }
      Exceptions.expectException(CompletionException.class, ClosedChannelException.class,
            () -> client().put(new byte[]{10, 20}, -1, -1, oversizedValue));

      killClient(hotRodClient);
      hotRodClient = connectClient();

      byte[] key = new byte[]{1, 2, 3};
      byte[] value = new byte[]{4, 5, 6};
      TestResponse putResp = client().put(key, -1, -1, value);
      HotRodTestingUtil.assertStatus(putResp, Success);
      assertSuccess(client().get(key, 0), value);
   }
}
