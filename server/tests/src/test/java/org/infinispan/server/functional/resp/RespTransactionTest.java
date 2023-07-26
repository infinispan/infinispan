package org.infinispan.server.functional.resp;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.vertx.core.Vertx;
import io.vertx.junit5.VertxTestContext;
import io.vertx.redis.client.RedisAPI;

public class RespTransactionTest extends AbstractRespTest {

   @Test
   public void testStartTxAndExecuteOperations(Vertx vertx, VertxTestContext ctx) {
      // Using STANDALONE mode here.
      RedisAPI server0 = createDirectConnection(0, vertx);
      RedisAPI server1 = createDirectConnection(1, vertx);

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
}
