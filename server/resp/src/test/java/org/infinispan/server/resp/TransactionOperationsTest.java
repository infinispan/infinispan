package org.infinispan.server.resp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.TimeUnit;

import org.testng.annotations.Test;

import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.TransactionResult;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;

@Test(groups = "functional", testName = "server.resp.TransactionOperationsTest")
public class TransactionOperationsTest extends SingleNodeRespBaseTest {

   @Test
   public void testStartTxAndQueueCommands() throws Exception {
      int numCommands = 20;
      RedisCommands<String, String> redis = redisConnection.sync();

      assertThat(redis.multi()).isEqualTo("OK");
      assertThat(redisConnection.isMulti()).isTrue();

      for (int i = 0; i < numCommands; i++) {
         // Lettuce returns `null` instead of QUEUED.
         assertThat(redis.set("k" + i, "v" + i)).isNull();
      }

      assertThat(redis.exec())
            .hasSize(numCommands)
            .allMatch("OK"::equals);
      assertThat(redisConnection.isMulti()).isFalse();

      for (int i = 0; i < numCommands; i++) {
         assertThat(redis.get("k" + i)).isEqualTo("v" + i);
      }
   }

   @Test
   public void testExecWithoutMulti() {
      RedisCommands<String, String> redis = redisConnection.sync();
      assertThat(redisConnection.isMulti()).isFalse();
      assertThatThrownBy(redis::exec)
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessage("ERR EXEC without MULTI");
   }

   @Test
   public void testEmptyTransaction() {
      RedisCommands<String, String> redis = redisConnection.sync();

      assertThat(redis.multi()).isEqualTo("OK");
      assertThat(redisConnection.isMulti()).isTrue();
      assertThat(redis.exec()).isEmpty();
      assertThat(redisConnection.isMulti()).isFalse();
   }

   @Test
   public void testTransactionWithError() {
      RedisCommands<String, String> redis = redisConnection.sync();

      assertThat(redis.multi()).isEqualTo("OK");
      assertThat(redisConnection.isMulti()).isTrue();

      redis.set("tx-err-k1", "v1");
      redis.hlen("tx-err-k1");
      redis.set("tx-err-k2", "v2");

      TransactionResult result = redis.exec();
      assertThat(result.<String>get(0)).isEqualTo("OK");
      assertThat(result.<Throwable>get(1))
            .hasMessage("ERRWRONGTYPE Operation against a key holding the wrong kind of value");
      assertThat(result.<String>get(2)).isEqualTo("OK");

      assertThat(redisConnection.isMulti()).isFalse();

      assertThat(redis.get("tx-err-k1")).isEqualTo("v1");
      assertThat(redis.get("tx-err-k2")).isEqualTo("v2");
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
