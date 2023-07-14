package org.infinispan.server.functional.resp;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxTestContext;
import io.vertx.redis.client.RedisAPI;
import io.vertx.redis.client.Response;
import io.vertx.redis.client.impl.types.ErrorType;
import io.vertx.redis.client.impl.types.MultiType;

public class RespListTest extends AbstractRespTest {

   @Test
   public void testRPUSH(Vertx vertx, VertxTestContext ctx) {
      RedisAPI redis = createConnection(vertx);
      redis.rpush(List.of("people", "tristan"))
            .onFailure(ctx::failNow)
            .compose(v -> {
               ctx.verify(() -> assertThat(v.toLong()).isEqualTo(1));
               return redis.rpush(List.of("people", "william"));
            })
            .compose(v -> {
               ctx.verify(() -> assertThat(v.toLong()).isEqualTo(2));
               return redis.rpush(List.of("people", "william", "jose", "pedro"));
            })
            .onSuccess(v -> ctx.verify(() -> assertThat(v.toLong()).isEqualTo(5)))
            .andThen(ignore -> redis.set(List.of("leads", "tristan")))
            .compose(ignore -> ctx.assertFailure(redis.rpush(List.of("leads", "william")))
                  .onFailure(t -> ctx.verify(() -> assertThat(t)
                        .isInstanceOf(ErrorType.class)
                        .hasMessageContaining("ERRWRONGTYPE")))
            ).onComplete(ignore -> ctx.completeNow());
   }

   @Test
   public void testConcurrentOperations(Vertx vertx, VertxTestContext ctx) {
      RedisAPI server0 = createDirectConnection(0, vertx);
      RedisAPI server1 = createDirectConnection(1, vertx);

      int size = 100;
      List<String> values = new ArrayList<>(size);
      List<Future<?>> futures = new ArrayList<>(size);

      CompletableFuture<Void> cs = new CompletableFuture<>();
      Future<?> await = Future.fromCompletionStage(cs);

      for (int i = 0; i < size; i++) {
         String value = "value" + i;
         values.add(value);

         Future<?> f;
         if ((i & 1) == 1) {
            f = await.compose(ignore -> server1.rpush(List.of("conc-dlist", value)));
         } else {
            f = await.compose(ignore -> server0.rpush(List.of("conc-dlist", value)));
         }
         futures.add(f);
      }

      cs.complete(null);
      Future.all(futures)
            .onFailure(ctx::failNow)
            .compose(ignore -> server0.lrange("conc-dlist", "0", "-1"))
            .onSuccess(v -> {
               ctx.verify(() -> assertThat(v)
                     .hasSize(size)
                     .isInstanceOfSatisfying(MultiType.class,
                           mt -> assertThat(mt.stream().map(Response::toString).allMatch(values::contains)).isTrue()));
               ctx.completeNow();
            });
   }
}
