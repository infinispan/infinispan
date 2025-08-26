package org.infinispan.server.functional.resp;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.vertx.core.Vertx;
import io.vertx.junit5.VertxTestContext;
import io.vertx.redis.client.RedisAPI;
import io.vertx.redis.client.RedisClusterTransactions;
import io.vertx.redis.client.RedisOptions;
import io.vertx.redis.client.impl.types.MultiType;

public class RespTransactionTest extends AbstractRespTest {

   @Test
   public void testStartTxAndExecuteOperations(Vertx vertx, VertxTestContext ctx) {
      // Using STANDALONE mode here.
      RedisAPI server0 = createDirectConnection(0, vertx, new RedisOptions().setClusterTransactions(RedisClusterTransactions.SINGLE_NODE));
      RedisAPI server1 = createDirectConnection(1, vertx, new RedisOptions().setClusterTransactions(RedisClusterTransactions.SINGLE_NODE));

      server0.multi()
            .onFailure(ctx::failNow)
            .compose(reply -> server0.set(List.of("tx-k1", "v1")))
            .compose(reply -> server0.set(List.of("tx-k2", "v2")))
            .compose(reply -> server0.get("tx-k2"))
            .compose(reply -> {
               ctx.verify(() -> assertThat(reply.toString()).isEqualTo("QUEUED"));

               // The transaction context exist only in the server0.
               // Redirecting the connection to a different server will not work.
               return server1.exec();
            })
            .recover(r -> {
               ctx.verify(() -> assertThat(r.toString()).isEqualTo("ERR EXEC without MULTI"));

               // Execute on the correct server.
               return server0.exec();
            })
            .onComplete(ctx.succeeding(reply -> {
               ctx.verify(() -> {
                  assertThat(reply).hasSize(3);
                  assertThat(reply.get(0).toString()).isEqualTo("OK");
                  assertThat(reply.get(1).toString()).isEqualTo("OK");
                  assertThat(reply.get(2).toString()).isEqualTo("v2");
               });
               ctx.completeNow();
            }));
   }

   @Test
   public void testTxAbortByWatcher(Vertx vertx, VertxTestContext ctx) {
      RedisAPI clustered = createConnection(vertx);
      RedisAPI standalone = createDirectConnection(0, vertx);
      standalone.watch(List.of("key-0", "key-1", "key-2"))
            .onFailure(ctx::failNow)
            .compose(reply -> standalone.multi())
            .compose(reply -> {
               // Standalone started a transaction.
               ctx.verify(() -> assertThat(reply.toString()).isEqualTo("OK"));
               return standalone.set(List.of("key-0", "value-0"));
            })
            .compose(reply -> {
               ctx.verify(() -> assertThat(reply.toString()).isEqualTo("QUEUED"));
               return standalone.set(List.of("key-1", "value-1"));
            })
            .compose(reply -> {
               ctx.verify(() -> assertThat(reply.toString()).isEqualTo("QUEUED"));
               return clustered.set(List.of("key-0", "outside-value-0"));
            })
            .compose(reply -> {
               // The clustered connection wrote to the key that is being watched.
               ctx.verify(() -> assertThat(reply.toString()).isEqualTo("OK"));
               return standalone.exec();
            })
            .compose(reply -> {
               // The transaction was not executed.
               ctx.verify(() -> assertThat(reply).isNull());
               return standalone.get("key-0");
            })
            .andThen(ctx.succeeding(reply -> {
               // The key has the value written by the clustered connection.
               ctx.verify(() -> assertThat(reply.toString()).isEqualTo("outside-value-0"));
               ctx.completeNow();
            }));
   }

   @Test
   public void testSuccessWithWatcher(Vertx vertx, VertxTestContext ctx) {
      RedisAPI clustered = createConnection(vertx);
      RedisAPI standalone = createDirectConnection(0, vertx);
      standalone.watch(List.of("key-0", "key-1", "key-2"))
            .onFailure(ctx::failNow)
            .compose(reply -> standalone.multi())
            .compose(reply -> {
               // Standalone started a transaction.
               ctx.verify(() -> assertThat(reply.toString()).isEqualTo("OK"));
               return standalone.set(List.of("key-0", "value-0"));
            })
            .compose(reply -> {
               ctx.verify(() -> assertThat(reply.toString()).isEqualTo("QUEUED"));
               return standalone.set(List.of("key-1", "value-1"));
            })
            .compose(reply -> {
               ctx.verify(() -> assertThat(reply.toString()).isEqualTo("QUEUED"));
               return clustered.set(List.of("other-key", "other-value"));
            })
            .compose(reply -> {
               ctx.verify(() -> assertThat(reply.toString()).isEqualTo("OK"));
               return standalone.exec();
            })
            .compose(reply -> {
               // Transaction executed correctly and returns a OK for each operation.
               ctx.verify(() -> assertThat(reply)
                     .isInstanceOfSatisfying(MultiType.class, mt ->
                        assertThat(mt)
                              .hasSize(2)
                              .allMatch(r -> r.toString().equals("OK"))
                     ));
               return standalone.get("key-0");
            })
            .andThen(ctx.succeeding(reply -> {
               ctx.verify(() -> assertThat(reply.toString()).isEqualTo("value-0"));
               ctx.completeNow();
            }));
   }

   @Test
   public void testTxSuccessWithFailedCommands(Vertx vertx, VertxTestContext ctx) {
      RedisAPI standalone = createDirectConnection(0, vertx);
      standalone.multi()
            .onFailure(ctx::failNow)
            .compose(reply -> {
               // Standalone started a transaction.
               ctx.verify(() -> assertThat(reply.toString()).isEqualTo("OK"));
               return standalone.set(List.of("key-0-w", "value-0"));
            })
            .compose(reply -> {
               ctx.verify(() -> assertThat(reply.toString()).isEqualTo("QUEUED"));
               return standalone.lpop(List.of("key-0-w"));
            })
            .compose(reply -> {
               ctx.verify(() -> assertThat(reply.toString()).isEqualTo("QUEUED"));
               return standalone.exec();
            })
            .compose(reply -> {
               // Transaction executed correctly. First operation OK and second fails.
               ctx.verify(() -> assertThat(reply)
                     .isInstanceOfSatisfying(MultiType.class, mt ->
                           assertThat(mt)
                                 .hasSize(2)
                                 .satisfies(ignore -> assertThat(mt.get(0).toString()).isEqualTo("OK"))
                                 .satisfies(ignore -> assertThat(mt.get(1).toString()).isEqualTo("WRONGTYPE Operation against a key holding the wrong kind of value"))
                     ));
               return standalone.get("key-0-w");
            })
            .andThen(ctx.succeeding(reply -> {
               // Key was still written successfully.
               ctx.verify(() -> assertThat(reply.toString()).isEqualTo("value-0"));
               ctx.completeNow();
            }));
   }
}
