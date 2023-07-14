package org.infinispan.server.functional.resp;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxTestContext;
import io.vertx.redis.client.RedisAPI;
import io.vertx.redis.client.Response;
import io.vertx.redis.client.impl.types.MultiType;

public class RespSetTest extends AbstractRespTest {

   @Test
   public void testSetOperations(Vertx vertx, VertxTestContext ctx) {
      RedisAPI redis = createConnection(vertx);

      redis.sadd(List.of("sadd", "1", "2", "3"))
            .onFailure(ctx::failNow)
            .compose(v -> {
               ctx.verify(() -> assertThat(v.toLong()).isEqualTo(3));
               return redis.sadd(List.of("sadd", "4", "5", "6"));
            })
            .compose(v -> {
               ctx.verify(() -> assertThat(v.toLong()).isEqualTo(3));
               return redis.sadd(List.of("sadd", "1", "2", "3", "4", "5", "6"));
            })
            .compose(v -> {
               ctx.verify(() -> assertThat(v.toLong()).isEqualTo(0));
               return redis.scard("sadd");
            })
            .compose(v -> {
               ctx.verify(() -> assertThat(v.toLong()).isEqualTo(6));
               return redis.smembers("sadd");
            })
            .onSuccess(v -> {
               ctx.verify(() -> assertThat(v).hasSize(6));
               ctx.completeNow();
            });
   }

   @Test
   public void testConcurrentOperations(Vertx vertx, VertxTestContext ctx) {
      RedisAPI server0 = createDirectConnection(0, vertx);
      RedisAPI server1 = createDirectConnection(1, vertx);

      int size = 100;
      Set<String> values = new HashSet<>(size);
      List<Future<?>> futures = new ArrayList<>(size);

      CompletableFuture<Void> cs = new CompletableFuture<>();
      Future<?> await = Future.fromCompletionStage(cs);

      for (int i = 0; i < size; i++) {
         String value = "value" + i;
         values.add(value);

         Future<?> f;
         if ((i & 1) == 1) {
            f = await.compose(ignore -> server1.sadd(List.of("conc-dset", value)));
         } else {
            f = await.compose(ignore -> server0.sadd(List.of("conc-dset", value)));
         }
         futures.add(f);
      }

      cs.complete(null);
      Future.all(futures)
            .onFailure(ctx::failNow)
            .compose(ignore -> server0.smembers("conc-dset"))
            .onSuccess(v -> {
               ctx.verify(() -> assertThat(v)
                     .hasSize(size)
                     .isInstanceOfSatisfying(MultiType.class,
                           mt -> assertThat(mt.stream().map(Response::toString).allMatch(values::contains)).isTrue()));
               ctx.completeNow();
            });
   }
}
