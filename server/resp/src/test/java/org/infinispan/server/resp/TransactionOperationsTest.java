package org.infinispan.server.resp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.infinispan.server.resp.test.RespTestingUtil.OK;
import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.v;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.infinispan.commons.test.skip.SkipTestNG;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.LockingMode;
import org.testng.annotations.Test;

import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.TransactionResult;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;

@Test(groups = "functional", testName = "server.resp.TransactionOperationsTest")
public class TransactionOperationsTest extends SingleNodeRespBaseTest {

   @Override
   protected void amendConfiguration(ConfigurationBuilder configurationBuilder) {
      configurationBuilder.invocationBatching().enable(true);
      configurationBuilder.transaction().lockingMode(LockingMode.PESSIMISTIC);
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
            .hasMessage("WRONGTYPE Operation against a key holding the wrong kind of value");
      assertThat(result.<String>get(2)).isEqualTo(OK);

      assertThat(redisConnection.isMulti()).isFalse();

      assertThat(redis.get("tx-err-k1")).isEqualTo("v1");
      assertThat(redis.get("tx-err-k2")).isEqualTo("v2");
   }

   @Test
   public void testStartNestedTx() throws Exception {
      StatefulRedisConnection<String, String> multi = newConnection();
      RedisCommands<String, String> redis = multi.sync();

      assertThat(redis.multi()).isEqualTo(OK);
      assertThat(multi.isMulti()).isTrue();

      assertThatThrownBy(redis::multi)
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessage("ERR MULTI calls can not be nested");

      multi.closeAsync().get(10, TimeUnit.SECONDS);
   }

   @Test(enabled = false, description = "redis/lettuce#3009")
   public void testWatchInMultiNotAbort() {
      RedisCommands<String, String> redis = redisConnection.sync();

      assertThat(redis.multi()).isEqualTo(OK);
      assertThat(redisConnection.isMulti()).isTrue();

      redis.set(k(), v());
      redis.set(k(1), v(1));

      // This returns an -ERR, but lettuce just returns null when in TX context.
      assertThat(redis.watch("something")).isNull();

      redis.set(k(2), v(2));

      TransactionResult result = redis.exec();
      assertThat(result.wasDiscarded()).isFalse();
      assertThat(result)
            .hasSize(3)
            .allMatch(OK::equals);

      for (int i = 0; i < 3; i++) {
         assertThat(redis.get(k(i))).isEqualTo(v(i));
      }
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
      StatefulRedisConnection<String, String> multi = newConnection();
      StatefulRedisConnection<String, String> outside = newConnection();

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
      StatefulRedisConnection<String, String> outside = newConnection();
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

   public void testBlpopNotBlocking() {
      RedisCommands<String, String> redis = redisConnection.sync();

      String key = k();
      String v0 = v();
      String v1 = v(1);

      // Add two entries to the list before starting the TX.
      assertThat(redis.lpush(key, v0, v1)).isEqualTo(2);

      assertThat(redis.multi()).isEqualTo(OK);
      assertThat(redisConnection.isMulti()).isTrue();

      // Pop 3 values from the list without any timeout.
      redis.blpop(0, key);
      redis.blpop(0, key);
      redis.blpop(0, key);

      // Execute transaction, the command should not block.
      TransactionResult result = redis.exec();
      assertThat(result.wasDiscarded()).isFalse();
      assertThat(result).hasSize(3);

      assertThat((Object) result.get(0))
            .isInstanceOfSatisfying(KeyValue.class, kv -> {
               assertThat(kv.getKey()).isEqualTo(key);
               assertThat(kv.getValue()).isEqualTo(v1);
            });

      assertThat((Object) result.get(1))
            .isInstanceOfSatisfying(KeyValue.class, kv -> {
               assertThat(kv.getKey()).isEqualTo(key);
               assertThat(kv.getValue()).isEqualTo(v0);
            });

      // Third pop returns null as there are no more values.
      assertThat((Object) result.get(2)).isNull();
   }

   public void testAbortBecauseOfError() {
      RedisCommands<String, String> redis = redisConnection.sync();

      assertThat(redis.multi()).isEqualTo(OK);
      assertThat(redisConnection.isMulti()).isTrue();

      assertThat(redis.set(k(), v())).isNull();

      // Command doesn't exist.
      redis.xadd(k(1), v(), v(1));

      assertThatThrownBy(redis::exec)
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessage("EXECABORT Transaction discarded because of previous errors.");

      assertThat(redis.get(k())).isNull();
   }

   public void testBlockingPopWithTx() throws Throwable {
      testBlockingPopWithTx(TestingUtil::k);
   }

   protected final void testBlockingPopWithTx(Supplier<String> keyFactory) throws Throwable {
      SkipTestNG.skipIf(!cache.getCacheConfiguration().transaction().transactionMode().isTransactional(), "Test does not have batching enabled.");
      String key = keyFactory.get();

      // Utilize a different connection for listener.
      RedisAsyncCommands<String, String> async = newConnection().async();
      RedisCommands<String, String> redis = redisConnection.sync();

      RedisFuture<KeyValue<String, String>> listener = async.blpop(0, key);

      assertThat(listener.isDone()).isFalse();

      assertThat(redis.multi()).isEqualTo("OK");
      assertThat(redisConnection.isMulti()).isTrue();

      // Add to the listened key
      redis.lpush(key, "value");

      // Listener is still blocked as transaction not executed.
      assertThat(listener.isDone()).isFalse();

      // We remove the queue. The listener should still be blocked.
      redis.del(key);

      // Before the commit, still not done!
      assertThat(listener.isDone()).isFalse();

      TransactionResult result = redis.exec();
      assertThat(result.wasDiscarded()).isFalse();
      assertThat(result).hasSize(2)
            .allMatch(elem -> elem.equals(1L));

      // Since the queue is removed, the listener still blocked.
      assertThat(listener.isDone()).isFalse();

      // Release listener.
      redis.lpush(key, "added-later");
      eventually(listener::isDone);

      KeyValue<String, String> kv = listener.get();
      assertThat(kv.getKey()).isEqualTo(key);
      assertThat(kv.getValue()).isEqualTo("added-later");
   }
}
