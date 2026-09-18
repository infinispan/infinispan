package org.infinispan.server.resp;

import static org.infinispan.server.resp.test.RespTestingUtil.createClient;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.infinispan.server.resp.configuration.RespServerConfigurationBuilder;
import org.infinispan.testing.Exceptions;
import org.testng.annotations.Test;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;

/**
 * RESP test to ensure requests larger than configured limits are not processed
 *
 * @author William Burns
 * @since 15.2
 */
@Test(groups = "functional", testName = "server.resp.RespRequestLimitTest")
public class RespRequestLimitTest extends SingleNodeRespBaseTest {
   private static final int MAX_BYTE_ARRAY_SIZE = 128;

   public RespRequestLimitTest() {
      // This way each test takes only 100 ms instead of 15s
      timeout = 100;
   }

   @Override
   protected RespServerConfigurationBuilder serverConfiguration(int i) {
      RespServerConfigurationBuilder builder = super.serverConfiguration(i);
      return builder.maxContentLength(Integer.toString(MAX_BYTE_ARRAY_SIZE));
   }

   public void testKeyTooLong() {
      RedisCommands<String, String> redis = redisConnection.sync();
      Exceptions.expectException(RedisCommandTimeoutException.class, () ->
            redis.set("k1".repeat((MAX_BYTE_ARRAY_SIZE / 2) + 1), "v1"));
   }

   public void testValueTooLong() {
      RedisCommands<String, String> redis = redisConnection.sync();
      Exceptions.expectException(RedisCommandTimeoutException.class, () ->
            redis.set("k1", "v1".repeat((MAX_BYTE_ARRAY_SIZE / 2) + 1)));
   }

   public void testArgumentsMax() {
      RedisCommands<String, String> redis = redisConnection.sync();
      Exceptions.expectException(RedisCommandTimeoutException.class, () ->
            // Only support up to 7 entries due to each key and value having /r/n and the size before it
            redis.mset(IntStream.range(0, 8)
                  .mapToObj(Integer::toString)
                  .collect(Collectors.toMap(e -> "k" + e, e -> "v" + e))));
   }

   /**
    * A request under the limit must still be accepted when the read it arrives in ends in the middle of it. The bytes
    * such a request consumed before the split used to be counted twice, so enough pipelined requests to span several
    * reads would see one rejected as soon as a split landed past the halfway mark of the limit.
    */
   public void testManyPipelinedRequestsNearLimit() throws Exception {
      // The class wide timeout is too tight for a batch this size
      RedisClient pipeliningClient = createClient(15_000, server.getPort());
      try (StatefulRedisConnection<String, String> connection = pipeliningClient.connect()) {
         RedisAsyncCommands<String, String> redis = connection.async();
         redis.setAutoFlushCommands(false);

         // Enough requests, each just under the limit, that they cannot all be delivered in a single read
         int opCount = 256;
         List<RedisFuture<String>> futures = new ArrayList<>(opCount);
         List<String> values = new ArrayList<>(opCount);
         for (int i = 0; i < opCount; ++i) {
            // Fixed width so that every request is the same size, 110 bytes once the RESP framing of a SET is added
            String key = String.format("k%03d", i);
            String value = Character.toString('a' + i % 26).repeat(80);
            values.add(value);
            futures.add(redis.set(key, value));
         }
         redis.flushCommands();

         for (int i = 0; i < opCount; ++i) {
            assertEquals("OK", futures.get(i).get(15, TimeUnit.SECONDS));
         }

         // Auto flush is a connection wide setting, the reads below are synchronous
         redis.setAutoFlushCommands(true);
         RedisCommands<String, String> sync = connection.sync();
         for (int i = 0; i < opCount; ++i) {
            assertEquals(values.get(i), sync.get(String.format("k%03d", i)));
         }
      } finally {
         pipeliningClient.shutdown();
      }
   }

   public void testExcessDataDoesNotCorruptSubsequentConnection() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String oversizedValue = "v".repeat(MAX_BYTE_ARRAY_SIZE * 4);
      Exceptions.expectException(RedisCommandTimeoutException.class, () -> redis.set("oversized", oversizedValue));

      redisConnection.close();
      redisConnection = newConnection();
      RedisCommands<String, String> fresh = redisConnection.sync();
      fresh.set("small", "hello");
      assertEquals("hello", fresh.get("small"));
   }
}
