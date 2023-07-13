package org.infinispan.server.functional.resp;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.vertx.core.Vertx;
import io.vertx.junit5.VertxTestContext;
import io.vertx.redis.client.RedisAPI;
import io.vertx.redis.client.impl.types.MultiType;

public class RespDistributedTest extends AbstractRespTest {

   @Test
   public void testBasicOperations(Vertx vertx, VertxTestContext ctx) {
      RedisAPI server0 = createDirectConnection(0, vertx);
      RedisAPI server1 = createDirectConnection(1, vertx);

      server0.set(List.of("dset", "dval"))
            .onFailure(ctx::failNow)
            .compose(v -> {
               ctx.verify(() -> assertThat(v.toString()).isEqualTo("OK"));
               return server1.get("dset");
            })
            .compose(v -> {
               ctx.verify(() -> assertThat(v.toString()).isEqualTo("dval"));
               return server0.get("dset");
            }).onSuccess(v -> {
               ctx.verify(() -> assertThat(v.toString()).isEqualTo("dval"));
               ctx.completeNow();
            });
   }

   @Test
   public void testHashOperations(Vertx vertx, VertxTestContext ctx) {
      RedisAPI server0 = createDirectConnection(0, vertx);
      RedisAPI server1 = createDirectConnection(1, vertx);

      server0.hset(List.of("dhash", "dkey", "dval"))
            .onFailure(ctx::failNow)
            .compose(v -> {
               ctx.verify(() -> assertThat(v.toLong()).isEqualTo(1));
               return server1.hget("dhash", "dkey");
            })
            .compose(v -> {
               ctx.verify(() -> assertThat(v.toString()).isEqualTo("dval"));
               return server0.hget("dhash", "dkey");
            }).onSuccess(v -> {
               ctx.verify(() -> assertThat(v.toString()).isEqualTo("dval"));
               ctx.completeNow();
            });
   }

   @Test
   public void testListOperations(Vertx vertx, VertxTestContext ctx) {
      RedisAPI server0 = createDirectConnection(0, vertx);
      RedisAPI server1 = createDirectConnection(1, vertx);

      server0.lpush(List.of("dlist", "dval"))
            .onFailure(ctx::failNow)
            .compose(v -> {
               ctx.verify(() -> assertThat(v.toLong()).isEqualTo(1));
               return server1.lrange("dlist", "0", "-1");
            })
            .compose(v -> {
               ctx.verify(() -> assertThat(v)
                     .hasSize(1)
                     .isInstanceOfSatisfying(MultiType.class,
                           mt -> assertThat(mt.get(0).toString()).isEqualTo("dval")));
               return server0.lrange("dlist", "0", "-1");
            }).onSuccess(v -> {
               ctx.verify(() -> assertThat(v)
                     .hasSize(1)
                     .isInstanceOfSatisfying(MultiType.class,
                           mt -> assertThat(mt.get(0).toString()).isEqualTo("dval")));
               ctx.completeNow();
            });
   }
}
