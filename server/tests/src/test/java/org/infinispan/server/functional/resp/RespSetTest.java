package org.infinispan.server.functional.resp;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.vertx.core.Vertx;
import io.vertx.junit5.VertxTestContext;
import io.vertx.redis.client.RedisAPI;

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
            .onSuccess(v -> ctx.verify(() -> assertThat(v.toLong()).isEqualTo(0)))
            .andThen(ignore -> redis.scard("sadd"))
            .compose(v -> {
               ctx.verify(() -> assertThat(v.toLong()).isEqualTo(6));
               return redis.smembers("sadd");
            })
            .onSuccess(v -> {
               ctx.verify(() -> assertThat(v).hasSize(6));
               ctx.completeNow();
            });
   }
}
