package org.infinispan.server.functional.resp;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.vertx.core.Vertx;
import io.vertx.junit5.VertxTestContext;
import io.vertx.redis.client.RedisAPI;
import io.vertx.redis.client.impl.types.ErrorType;

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
}
