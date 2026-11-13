package org.infinispan.server.hotrod.test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.server.hotrod.OperationStatus.KeyDoesNotExist;
import static org.infinispan.server.hotrod.OperationStatus.Success;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.assertStatus;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.assertSuccess;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.killClient;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.nio.channels.ClosedChannelException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

import org.infinispan.commons.util.SaslUtils;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.core.security.simple.SimpleAuthenticator;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.HotRodSingleNodeTest;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.testing.Exceptions;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.embedded.EmbeddedChannel;

/**
 * HotRod test to ensure requests larger than configured limits are not processed
 *
 * @author William Burns
 * @since 15.2
 */
@Test(groups = "functional", testName = "server.hotrod.HotRodRequestLimitTest")
public class HotRodRequestLimitTest extends HotRodSingleNodeTest {
   private static final int MAX_CONTENT_LENGTH = 128;
   private static final String OVERSIZE_MARKER = "OVERSIZE-SHOULD-FAIL-";
   // A SCRAM exchange does not fit in the limit below, its final message alone is larger than that
   private static final String MECH = "CRAM-MD5";
   private static final String USER = "user";
   private static final String REALM = "realm";
   private static final String PASSWORD = "password";

   private boolean authentication;

   public HotRodRequestLimitTest authentication(boolean authentication) {
      this.authentication = authentication;
      return this;
   }

   @Factory
   public Object[] factory() {
      return new Object[] {
            new HotRodRequestLimitTest().authentication(false),
            // The authentication exchange goes through the same decoder as everything else, so it has to fit in the
            // limit and must not leave any of its bytes charged against the operations that follow it
            new HotRodRequestLimitTest().authentication(true),
      };
   }

   @Override
   protected String parameters() {
      return "[auth=" + authentication + "]";
   }

   @AfterMethod
   public void restartClient() {
      killClient(hotRodClient);
      hotRodClient = connectClient();
   }

   protected HotRodServer createStartHotRodServer(EmbeddedCacheManager cacheManager) {
      HotRodServerConfigurationBuilder builder = new HotRodServerConfigurationBuilder()
            .maxContentLength(Integer.toString(MAX_CONTENT_LENGTH));
      if (authentication) {
         SimpleAuthenticator authenticator = new SimpleAuthenticator();
         authenticator.addUser(USER, REALM, PASSWORD.toCharArray());
         builder.authentication().enable()
               .sasl()
               .authenticator(authenticator)
               .addAllowedMech(MECH)
               .serverName("localhost")
               .addMechProperty(Sasl.POLICY_NOANONYMOUS, "true");
      }
      // The test handlers install a 1 byte frame decoder, which means a request is never handed to the decoder in
      // the same read as another one. The pipelining tests below rely on real reads to spot bytes leaking from one
      // request into the next
      return HotRodTestingUtil.startHotRodServer(cacheManager, HotRodTestingUtil.host(), HotRodTestingUtil.serverPort(),
            builder, false);
   }

   @Override
   protected HotRodClient connectClient(byte protocolVersion) {
      HotRodClient client = super.connectClient(protocolVersion);
      if (authentication) {
         // Every connection has to authenticate before it may run an operation, including the ones the tests below
         // open again after the server dropped the previous one
         try {
            SaslClient sc = SaslUtils.getSaslClientFactory(getClass().getClassLoader(), MECH)
                  .createSaslClient(new String[]{MECH}, null, "hotrod", "localhost", new HashMap<>(),
                        new TestCallbackHandler(USER, REALM, PASSWORD));
            assertThat(client.auth(sc)).isInstanceOf(TestAuthResponse.class);
         } catch (SaslException e) {
            throw new AssertionError("Could not authenticate the client", e);
         }
      }
      return client;
   }

   public void testKeyTooLong() {
      byte[] key = new byte[MAX_CONTENT_LENGTH + 2];
      Exceptions.expectException(CompletionException.class, ClosedChannelException.class,
            () -> client().put(key, -1, -1, new byte[]{1, 3, 4}));
      assertCacheEmpty();
   }

   public void testValueTooLong() {
      byte[] value = new byte[MAX_CONTENT_LENGTH + 2];
      Exceptions.expectException(CompletionException.class, ClosedChannelException.class,
            () -> client().put(new byte[]{1, 3, 4}, -1, -1, value));
      assertCacheEmpty();
   }

   public void testLongCacheName() {
      Exceptions.expectException(CompletionException.class, ClosedChannelException.class,
            () -> client().execute(0xA0, (byte) 0x01, "1".repeat(MAX_CONTENT_LENGTH + 1),
                  new byte[]{23}, -1, -1, new byte[]{23}, 0, (byte) 1, 0));
      assertCacheEmpty();
   }

   public void testWithManyKeysTotalLarger() {
      int entrySize = 4;
      byte[] value = new byte[entrySize - 2]; // keys are two bytes
      Arrays.fill(value, (byte) 1);
      RemoteTransaction tx = RemoteTransaction.startTransaction(client());
      for (byte i = 0; i < MAX_CONTENT_LENGTH / entrySize; ++i) {
         tx.set(new byte[]{i, (byte) (i + 1)}, value);
      }
      Exceptions.expectException(CompletionException.class, ClosedChannelException.class, tx::prepare);
      assertCacheEmpty();
   }

   public void testTooManyKeys() {
      Map<byte[], byte[]> map = new HashMap<>();
      for (byte i = 0; i < 10; i++) {
         map.put(new byte[]{i, (byte) (i + 1)}, new byte[]{i, (byte) (i + 1)});
      }
      Exceptions.expectException(CompletionException.class, ClosedChannelException.class, () -> client().putAll(map, -1, -1));
      assertCacheEmpty();
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

      assertCacheEmpty();

      byte[] key = new byte[]{1, 2, 3};
      byte[] value = new byte[]{4, 5, 6};
      TestResponse putResp = client().put(key, -1, -1, value);
      assertStatus(putResp, Success);
      assertSuccess(client().get(key, 0), value);
   }

   /**
    * Regression test to ensure the byte counter used to enforce max-content-length is reset between requests and
    * is not double counted while a single request is spread over several reads.
    * <p>
    * The bug: the counter was incremented by everything a decode invocation consumed, including the tail of an
    * already completed pipelined request, while the amount read since the start of the current request was added on
    * top of it. A request that fits in the limit was therefore rejected once it was preceded by another request on
    * the same connection, or once it was delivered in more than one read.
    */
   public void testPipelinedSmallRequestAfterLargeRequest() throws ExecutionException, InterruptedException, TimeoutException {
      HotRodClient client = client();

      // A put of a two byte key into "defaultcache" costs 35 bytes on top of the value (header, cache name, key,
      // expiration and the value length). Size the first value so the request is just under MAX_CONTENT_LENGTH:
      // large enough that the next request has no budget left if the bytes are charged to it, small enough to be
      // accepted on its own.
      byte[] firstKey = new byte[]{1, 2};
      byte[] firstValue = new byte[MAX_CONTENT_LENGTH - 40];
      Arrays.fill(firstValue, (byte) 1);

      Op firstOp = new Op(0xA0, client.protocolVersion(), (byte) 0x01, client.defaultCacheName(),
            firstKey, -1, -1, firstValue, 0, 0, (byte) 1, 0);

      // Second put: comfortably below the limit on its own, it may only be rejected if the first request's bytes
      // are charged against it
      byte[] secondKey = new byte[]{3, 4};
      byte[] secondValue = new byte[]{5, 6, 7, 8};

      Op secondOp = new Op(0xA0, client.protocolVersion(), (byte) 0x01, client.defaultCacheName(),
            secondKey, -1, -1, secondValue, 0, 0, (byte) 1, 0);

      // Pipeline both operations - this ensures they're written together
      // so the second request's bytes arrive in the same socket read as the tail of the first
      client.writeOps(firstOp, secondOp)
            .get(10, TimeUnit.SECONDS);

      ClientHandler handler = (ClientHandler) client.getChannel().pipeline().last();
      // Channel can be killed before we even get the response
      if (handler == null) {
         assertFalse(client.getChannel().isActive());
         return;
      }

      // The first operation should work fine and write the entry
      CompletionStage<TestResponse> firstResponseStage = handler.waitForResponse(firstOp.id);
      CompletionStage<TestResponse> secondResponseStage = handler.waitForResponse(secondOp.id);

      TestResponse firstResponse = firstResponseStage.toCompletableFuture().get(client.rspTimeoutSeconds, TimeUnit.SECONDS);
      assertStatus(firstResponse, Success);
      // The second operation should also succeed since it's small
      TestResponse secondResponse = secondResponseStage.toCompletableFuture().get(client.rspTimeoutSeconds, TimeUnit.SECONDS);
      assertStatus(secondResponse, Success);
      // Verify both entries were actually written
      restartClient();
      assertSuccess(client().get(firstKey, 0), firstValue);
      assertSuccess(client().get(secondKey, 0), secondValue);
   }

   /**
    * Same regression as {@link #testPipelinedSmallRequestAfterLargeRequest()}, but with enough pipelined requests to
    * span several reads. A read then routinely ends in the middle of a request, so the bytes of the requests that
    * completed earlier in that same read are the ones charged against the request that is still being parsed.
    */
   public void testManyPipelinedRequestsNearLimit() throws ExecutionException, InterruptedException, TimeoutException {
      HotRodClient client = client();
      ClientHandler handler = (ClientHandler) client.getChannel().pipeline().last();

      // Enough requests, each just under the limit, that they cannot all be delivered in a single read
      int opCount = 256;
      Op[] ops = new Op[opCount];
      byte[][] keys = new byte[opCount][];
      byte[][] values = new byte[opCount][];
      List<CompletionStage<TestResponse>> responseStages = new ArrayList<>(opCount);
      for (int i = 0; i < opCount; ++i) {
         keys[i] = new byte[]{(byte) i, (byte) (i >> 8)};
         values[i] = new byte[MAX_CONTENT_LENGTH - 40];
         Arrays.fill(values[i], (byte) i);
         ops[i] = new Op(0xA0, client.protocolVersion(), (byte) 0x01, client.defaultCacheName(),
               keys[i], -1, -1, values[i], 0, 0, (byte) 1, 0);
         // The responses have to be awaited before writing, a response that arrives first is simply dropped
         responseStages.add(handler.waitForResponse(ops[i].id));
      }

      client.writeOps(ops).get(10, TimeUnit.SECONDS);

      for (int i = 0; i < opCount; ++i) {
         TestResponse response = responseStages.get(i).toCompletableFuture().get(client.rspTimeoutSeconds, TimeUnit.SECONDS);
         assertStatus(response, Success);
      }

      restartClient();
      for (int i = 0; i < opCount; ++i) {
         assertSuccess(client().get(keys[i], 0), values[i]);
      }
   }

   /**
    * The leftover bytes of a request rejected for exceeding the maximum content length must never be decoded as the
    * continuation of that request: the first byte of the payload would be read as the length of a much shorter value,
    * storing a truncated entry under the key that was already parsed.
    */
   public void testOversizedValueDoesNotStoreTruncatedEntry() {
      byte[] key = "mcl-oversize-put".getBytes(StandardCharsets.UTF_8);
      byte[] value = oversizedValue(MAX_CONTENT_LENGTH * 4);

      Exceptions.expectException(CompletionException.class, ClosedChannelException.class,
            () -> client().put(key, -1, -1, value));

      killClient(hotRodClient);
      hotRodClient = connectClient();

      assertStatus(client().get(key, 0), KeyDoesNotExist);
      assertFalse(advancedCache.containsKey(key), "A truncated entry was stored for the rejected request");
      assertEquals(0, advancedCache.size(), "The rejected request must not have stored anything");
   }

   /**
    * Same as {@link #testOversizedValueDoesNotStoreTruncatedEntry()}, but the leftover bytes are delivered to the
    * decoder by reads that happen after the connection close has been initiated.
    */
   public void testOversizedValueSpanningMultipleReadsDoesNotStoreTruncatedEntry() {
      byte[] key = "mcl-oversize-put-chunked".getBytes(StandardCharsets.UTF_8);
      // Larger than the biggest buffer Netty allocates for a single read, so the payload spans several of them
      byte[] value = oversizedValue(64 * 1024);

      Exceptions.expectException(CompletionException.class, ClosedChannelException.class,
            () -> client().put(key, -1, -1, value));

      killClient(hotRodClient);
      hotRodClient = connectClient();

      assertStatus(client().get(key, 0), KeyDoesNotExist);
      assertEquals(0, advancedCache.size(), "The rejected request must not have stored anything");
   }

   /**
    * When an oversized operation is pipelined after a valid one, the entry written by the valid operation must not be
    * overwritten by a truncated value decoded from the leftover bytes of the rejected one.
    */
   public void testPipelinedOversizedOperationDoesNotOverwriteEntry() throws ExecutionException, InterruptedException,
         TimeoutException {
      HotRodClient client = client();
      byte[] key = new byte[]{1, 2};
      byte[] value = new byte[]{3, 4};

      Op firstOp = new Op(0xA0, client.protocolVersion(), (byte) 0x01, client.defaultCacheName(),
            key, -1, -1, value, 0, 0, (byte) 1, 0);
      Op tooLongOp = new Op(0xA0, client.protocolVersion(), (byte) 0x01, client.defaultCacheName(),
            key, -1, -1, oversizedValue(MAX_CONTENT_LENGTH * 4), 0, 0, (byte) 1, 0);

      client.writeOps(firstOp, tooLongOp).get(10, TimeUnit.SECONDS);

      killClient(hotRodClient);
      hotRodClient = connectClient();

      // The first operation may or may not have completed before the connection was closed, but the only value that
      // can ever be stored is the one it carried
      TestGetResponse response = client().get(key, 0);
      if (response.getStatus() == Success) {
         assertSuccess(response, value);
      } else {
         assertStatus(response, KeyDoesNotExist);
      }
   }

   /**
    * Resetting the parser state is not enough on its own: the bytes left over from the rejected request have to be
    * dropped too. Here the payload of the oversized value is itself a well formed Hot Rod request, so a decoder that
    * merely starts parsing from scratch would happily execute the smuggled operation on a connection that the server
    * has already decided to close.
    */
   public void testRequestSmuggledInOversizedValueIsNotExecuted() {
      HotRodClient client = client();
      byte[] smuggledKey = "mcl-smuggled-key".getBytes(StandardCharsets.UTF_8);
      Op smuggledOp = new Op(0xA0, client.protocolVersion(), (byte) 0x01, client.defaultCacheName(),
            smuggledKey, 0, 0, "mcl-smuggled-value".getBytes(StandardCharsets.UTF_8), 0, 0, (byte) 1, 0);

      // The decoder resumes right at the first byte of the rejected payload, which here is the magic of a complete
      // request. The rest is padding that cannot be parsed as anything.
      byte[] value = Arrays.copyOf(encode(client.protocolVersion(), smuggledOp), MAX_CONTENT_LENGTH * 4);

      Exceptions.expectException(CompletionException.class, ClosedChannelException.class,
            () -> client().put("mcl-smuggler".getBytes(StandardCharsets.UTF_8), -1, -1, value));

      killClient(hotRodClient);
      hotRodClient = connectClient();

      assertStatus(client().get(smuggledKey, 0), KeyDoesNotExist);
      assertEquals(0, advancedCache.size(), "The rejected request must not have stored anything");
   }

   private static byte[] encode(byte protocolVersion, Op op) {
      EmbeddedChannel channel = new EmbeddedChannel(new Encoder(protocolVersion));
      try {
         assertTrue(channel.writeOutbound(op));
         ByteBuf encoded = channel.readOutbound();
         try {
            return ByteBufUtil.getBytes(encoded);
         } finally {
            encoded.release();
         }
      } finally {
         channel.finishAndReleaseAll();
      }
   }

   private void assertCacheEmpty() {
      // Reconnecting round-trips with the server, so anything the rejected request could have triggered has been
      // processed by the time the cache is inspected
      killClient(hotRodClient);
      hotRodClient = connectClient();
      assertEquals(0, advancedCache.size(), "The rejected request must not have stored anything");
   }

   /**
    * A payload whose bytes are all valid single byte vInts, so that any of them left in the decoder buffer would be
    * happily read as the length of a shorter array. The first one is {@code 'O'}, i.e. 79.
    */
   private static byte[] oversizedValue(int size) {
      byte[] value = new byte[size];
      byte[] marker = OVERSIZE_MARKER.getBytes(StandardCharsets.UTF_8);
      for (int i = 0; i < value.length; i++) {
         value[i] = marker[i % marker.length];
      }
      return value;
   }
}
