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
import io.vertx.redis.client.impl.types.MultiType;

public class RespStringTest extends AbstractRespTest {

   @Test
   public void testSetGetDelete(Vertx vertx, VertxTestContext ctx) {
      RedisAPI redis = createConnection(vertx);
      redis.set(List.of("k1", "v1"))
            .onFailure(ctx::failNow)
            .compose(ignore -> redis.get("k1").onFailure(ctx::failNow))
            .onSuccess(v -> ctx.verify(() -> assertThat(v.toString()).isEqualTo("v1")))
            .compose(ignore -> redis.del(List.of("k1")).onFailure(ctx::failNow))
            .compose(ignore -> redis.get("k1").onFailure(ctx::failNow))
            .onFailure(ctx::failNow)
            .onSuccess(v -> {
               ctx.verify(() -> assertThat(v).isNull());
               ctx.completeNow();
            });
   }

   @Test
   public void testBasicOperationsConcurrently(Vertx vertx, VertxTestContext ctx) {
      RedisAPI server0 = createDirectConnection(0, vertx);
      RedisAPI server1 = createDirectConnection(1, vertx);

      int size = 100;
      List<Future<?>> futures = new ArrayList<>(size);
      List<String> keys = new ArrayList<>(size);
      List<String> values = new ArrayList<>(size);

      CompletableFuture<Void> cs = new CompletableFuture<>();
      Future<?> await = Future.fromCompletionStage(cs);

      for (int i = 0; i < size; i++) {
         String value = "value" + i;
         String key = "conc-basic-set-" + i;
         keys.add(key);
         values.add(value);

         Future<?> f;
         if ((i & 1) == 1) {
            f = await.compose(ignore -> server1.set(List.of(key, value)));
         } else {
            f = await.compose(ignore -> server0.set(List.of(key, value)));
         }
         futures.add(f);
      }

      cs.complete(null);
      Future.all(futures)
            .onFailure(ctx::failNow)
            .compose(ignore -> server0.mget(keys))
            .onSuccess(v -> {
               ctx.verify(() -> assertThat(v)
                     .hasSize(size)
                     .isInstanceOfSatisfying(MultiType.class,
                           mt -> assertThat(mt.stream().map(Response::toString).allMatch(values::contains)).isTrue()));
               ctx.completeNow();
            });
   }
}
