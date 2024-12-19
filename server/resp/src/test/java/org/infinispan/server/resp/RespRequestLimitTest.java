package org.infinispan.server.resp;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.infinispan.commons.test.Exceptions;
import org.infinispan.server.resp.configuration.RespServerConfigurationBuilder;
import org.testng.annotations.Test;

import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.api.sync.RedisCommands;

/**
 * RESP test to ensure requests larger than configured limits are not processed
 *
 * @author William Burns
 * @since 15.1
 */
@Test(groups = "functional", testName = "server.resp.RespRequestLimitTest")
public class RespRequestLimitTest extends SingleNodeRespBaseTest {
   private static final int MAX_BYTE_ARRAY_SIZE = 128;
   private static final int MAX_KEY_COUNT = 10;

   public RespRequestLimitTest() {
      // This way each test takes only 100 ms instead of 15s
      timeout = 100;
   }

   @Override
   protected RespServerConfigurationBuilder serverConfiguration(int i) {
      RespServerConfigurationBuilder builder = super.serverConfiguration(i);
      return builder.maxByteArraySize(MAX_BYTE_ARRAY_SIZE)
            .maxKeyCount(MAX_KEY_COUNT);
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
      // Note this must be 9 or greater (note this multiplied by 2 since we write key and value)
      // as the redis protocol reuses a 16 sized List to avoid allocations
      Exceptions.expectException(RedisCommandTimeoutException.class, () ->
            redis.mset(IntStream.range(0, 9)
                  .mapToObj(Integer::toString)
                  .collect(Collectors.toMap(e -> "k" + e, e -> "v" + e))));
   }
}
