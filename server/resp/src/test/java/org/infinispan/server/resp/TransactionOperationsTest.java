package org.infinispan.server.resp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.TimeUnit;

import org.testng.annotations.Test;

import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;

@Test(groups = "functional", testName = "server.resp.TransactionOperationsTest")
public class TransactionOperationsTest extends SingleNodeRespBaseTest {

   @Test
   public void testStartTxAndQueueCommands() throws Exception {
      StatefulRedisConnection<String, String> multi = client.connect();
      RedisCommands<String, String> redis = multi.sync();

      assertThat(redis.multi()).isEqualTo("OK");
      assertThat(multi.isMulti()).isTrue();

      for (int i = 0; i < 10; i++) {
         // Lettuce returns `null` instead of QUEUED.
         assertThat(redis.set("k" + i, "v" + i)).isNull();
      }

      // TODO: use EXEC to apply
      multi.closeAsync().get(10, TimeUnit.SECONDS);
   }

   @Test
   public void testStartNestedTx() throws Exception {
      StatefulRedisConnection<String, String> multi = client.connect();
      RedisCommands<String, String> redis = multi.sync();

      assertThat(redis.multi()).isEqualTo("OK");
      assertThat(multi.isMulti()).isTrue();

      assertThatThrownBy(redis::multi)
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessage("ERR MULTI calls can not be nested");

      multi.closeAsync().get(10, TimeUnit.SECONDS);
   }
}
