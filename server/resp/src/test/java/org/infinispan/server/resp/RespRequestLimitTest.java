package org.infinispan.server.resp;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.infinispan.server.resp.configuration.RespServerConfigurationBuilder;
import org.infinispan.testing.Exceptions;
import org.testng.annotations.Test;

import io.lettuce.core.RedisCommandTimeoutException;
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
}
