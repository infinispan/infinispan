package org.infinispan.server.functional.resp;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.vertx.core.Vertx;
import io.vertx.junit5.VertxTestContext;
import io.vertx.redis.client.RedisAPI;

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
}
