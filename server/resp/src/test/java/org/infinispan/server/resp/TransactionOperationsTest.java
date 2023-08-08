package org.infinispan.server.resp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.infinispan.server.resp.test.RespTestingUtil.OK;

import java.util.concurrent.TimeUnit;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.TransactionResult;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;

@Test(groups = "functional", testName = "server.resp.TransactionOperationsTest")
public class TransactionOperationsTest extends SingleNodeRespBaseTest {

   @Override
   protected void amendConfiguration(ConfigurationBuilder configurationBuilder) {
      configurationBuilder.invocationBatching().enable(true);
   }

   @Test
   public void testStartTxAndQueueCommands() throws Exception {
      int numCommands = 20;
      RedisCommands<String, String> redis = redisConnection.sync();

      assertThat(redis.multi()).isEqualTo(OK);
      assertThat(redisConnection.isMulti()).isTrue();

      for (int i = 0; i < numCommands; i++) {
         // Lettuce returns `null` instead of QUEUED.
         assertThat(redis.set("k" + i, "v" + i)).isNull();
      }

      assertThat(redis.exec())
            .hasSize(numCommands)
            .allMatch(OK::equals);
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

      assertThat(redis.multi()).isEqualTo(OK);
      assertThat(redisConnection.isMulti()).isTrue();
      assertThat(redis.exec()).isEmpty();
      assertThat(redisConnection.isMulti()).isFalse();
   }

   @Test
   public void testTransactionWithError() {
      RedisCommands<String, String> redis = redisConnection.sync();

      assertThat(redis.multi()).isEqualTo(OK);
      assertThat(redisConnection.isMulti()).isTrue();

      redis.set("tx-err-k1", "v1");
      redis.hlen("tx-err-k1");
      redis.set("tx-err-k2", "v2");

      TransactionResult result = redis.exec();
      assertThat(result.<String>get(0)).isEqualTo(OK);
      assertThat(result.<Throwable>get(1))
            .hasMessage("ERRWRONGTYPE Operation against a key holding the wrong kind of value");
      assertThat(result.<String>get(2)).isEqualTo(OK);

      assertThat(redisConnection.isMulti()).isFalse();

      assertThat(redis.get("tx-err-k1")).isEqualTo("v1");
      assertThat(redis.get("tx-err-k2")).isEqualTo("v2");
   }

   @Test
   public void testStartNestedTx() throws Exception {
      StatefulRedisConnection<String, String> multi = client.connect();
      RedisCommands<String, String> redis = multi.sync();

      assertThat(redis.multi()).isEqualTo(OK);
      assertThat(multi.isMulti()).isTrue();

      assertThatThrownBy(redis::multi)
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessage("ERR MULTI calls can not be nested");

      multi.closeAsync().get(10, TimeUnit.SECONDS);
   }

   @Test
   public void testTransactionAbortWithError() {
      RedisCommands<String, String> redis = redisConnection.sync();

      assertThat(redis.multi()).isEqualTo(OK);
      assertThat(redisConnection.isMulti()).isTrue();

      // This returns an -ERR, but lettuce just returns null when in TX context.
      assertThat(redis.watch("something")).isNull();


      assertThatThrownBy(redis::exec)
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessage("EXECABORT Transaction discarded because of previous errors.");
   }

   @Test
   public void testTransactionWithWatcher() {
      RedisCommands<String, String> redis = redisConnection.sync();

      assertThat(redis.watch("tx-watcher-k1")).isEqualTo(OK);
      assertThat(redis.multi()).isEqualTo(OK);
      assertThat(redisConnection.isMulti()).isTrue();

      assertThat(redis.set("tx-watcher-k1", "value")).isNull();

      TransactionResult result = redis.exec();
      assertThat(result.<String>get(0)).isEqualTo(OK);

      assertThat(redisConnection.isMulti()).isFalse();

      assertThat(redis.get("tx-watcher-k1")).isEqualTo("value");
   }

   @Test
   public void testWatcherCapturesChange() {
      testMultiWithWatcher(false);
   }

   @Test
   public void testRemovingWatcherBeforeExec() {
      testMultiWithWatcher(true);
   }

   private void testMultiWithWatcher(boolean unwatchBeforeExec) {
      // Create two different connections. In CLI, need to open two windows.
      StatefulRedisConnection<String, String> multi = client.connect();
      StatefulRedisConnection<String, String> outside = client.connect();

      RedisCommands<String, String> tx = multi.sync();
      RedisCommands<String, String> redis = outside.sync();

      // Start a watcher on the client which will execute the TX.
      assertThat(tx.watch("tx-watcher-key")).isEqualTo(OK);

      // Another client writes the key.
      assertThat(redis.set("tx-watcher-key", "value-outside")).isEqualTo(OK);

      // UNWATCH before entering multi context. Now it should proceed even with changes.
      // The client that issue the watch is the one who needs to unwatch. Watching a key is not global.
      if (unwatchBeforeExec) {
         assertThat(tx.unwatch()).isEqualTo(OK);
      }

      assertThat(tx.multi()).isEqualTo(OK);
      assertThat(multi.isMulti()).isTrue();
      assertThat(outside.isMulti()).isFalse();

      // Client in MULTI queues a write.
      assertThat(tx.set("tx-watcher-key", "value-inside")).isNull();

      TransactionResult result = tx.exec();
      assertThat(result.wasDiscarded()).isEqualTo(!unwatchBeforeExec);
      assertThat(multi.isMulti()).isFalse();

      // If watch in place, the TX abort and only the outside write is visible.
      // If watcher removed, the TX succeeds and the inside write is visible.
      String expected = unwatchBeforeExec ? "value-inside" : "value-outside";
      assertThat(redis.get("tx-watcher-key")).isEqualTo(expected);
      assertThat(tx.get("tx-watcher-key")).isEqualTo(expected);
   }

   @Test
   public void testDiscardWithoutMulti() {
      RedisCommands<String, String> redis = redisConnection.sync();
      assertThat(redisConnection.isMulti()).isFalse();
      assertThatThrownBy(redis::discard)
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessage("ERR DISCARD without MULTI");
   }

   @Test
   public void testDiscardingTransaction() {
      RedisCommands<String, String> redis = redisConnection.sync();

      assertThat(redis.multi()).isEqualTo("OK");
      assertThat(redisConnection.isMulti()).isTrue();

      assertThat(redis.set("tx-discard-k1", "value")).isNull();
      assertThat(redis.discard()).isEqualTo("OK");

      assertThat(redisConnection.isMulti()).isFalse();
      assertThatThrownBy(redis::exec)
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessage("ERR EXEC without MULTI");
      assertThat(redis.get("tx-discard-k1")).isNull();
   }

   @Test
   public void testDiscardRemoveListeners() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Install watch before multi.
      assertThat(redis.watch("tx-discard-k2")).isEqualTo("OK");

      assertThat(redis.multi()).isEqualTo("OK");
      assertThat(redisConnection.isMulti()).isTrue();

      assertThat(redis.set("tx-discard-k2", "value")).isNull();

      // Discard and removes the listener.
      assertThat(redis.discard()).isEqualTo("OK");

      assertThat(redisConnection.isMulti()).isFalse();

      // Enter into multi again.
      assertThat(redis.multi()).isEqualTo("OK");
      assertThat(redisConnection.isMulti()).isTrue();

      assertThat(redis.set("tx-discard-k2", "value-inside")).isNull();

      // Another client writes the key. Originally, this would notify the listener.
      StatefulRedisConnection<String, String> outside = client.connect();
      RedisCommands<String, String> outsideSync = outside.sync();
      assertThat(outsideSync.set("tx-discard-k2", "value-outside")).isEqualTo("OK");
      assertThat(outsideSync.get("tx-discard-k2")).isEqualTo("value-outside");
      outside.close();

      // Since the listener was removed with the discard. The operation will complete successfully.
      TransactionResult result = redis.exec();
      assertThat(result.wasDiscarded()).isFalse();
      assertThat(redisConnection.isMulti()).isFalse();
      assertThat(redis.get("tx-discard-k2")).isEqualTo("value-inside");
   }
}
