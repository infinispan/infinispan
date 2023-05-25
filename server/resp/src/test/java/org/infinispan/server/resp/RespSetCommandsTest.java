package org.infinispan.server.resp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.testng.annotations.Test;

import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.api.sync.RedisCommands;

@Test(groups = "functional", testName = "server.resp.RespSetCommandsTest")
public class RespSetCommandsTest extends SingleNodeRespBaseTest {
    @Test
    public void testSadd() {
        RedisCommands<String, String> redis = redisConnection.sync();
        String key = "sadd";
        Long newValue = redis.sadd(key, "1", "2", "3");
        assertThat(newValue.longValue()).isEqualTo(3L);
        newValue = redis.sadd(key, "4", "5");
        assertThat(newValue.longValue()).isEqualTo(2L);
        newValue = redis.sadd(key, "5", "6");
        assertThat(newValue.longValue()).isEqualTo(1L);

        // SADD on an existing key that contains a String, not a Set!
        // Set a String Command
        redis.set("leads", "tristan");
        assertThatThrownBy(() -> {
            redis.lpushx("leads", "william");
        }).isInstanceOf(RedisCommandExecutionException.class)
                .hasMessageContaining("ERRWRONGTYPE");
        // SADD on an existing key that contains a List, not a Set!
        // Create a List
        redis.rpush("listleads", "tristan");
        assertThatThrownBy(() -> {
            redis.lpushx("leads", "william");
        }).isInstanceOf(RedisCommandExecutionException.class)
                .hasMessageContaining("ERRWRONGTYPE");
    }
}
