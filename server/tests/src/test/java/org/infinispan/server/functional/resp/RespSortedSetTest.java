package org.infinispan.server.functional.resp;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.vertx.core.Vertx;
import io.vertx.junit5.VertxTestContext;
import io.vertx.redis.client.RedisAPI;
import io.vertx.redis.client.impl.types.MultiType;

public class RespSortedSetTest extends AbstractRespTest {

   @Test
   public void testSortedSetOperations(Vertx vertx, VertxTestContext ctx) {
      RedisAPI redis = createConnection(vertx);

      redis.zadd(List.of("zadd", "10.4", "v1"))
            .onFailure(ctx::failNow)
            .compose(v -> {
               ctx.verify(() -> assertThat(v.toLong()).isEqualTo(1));
               return redis.zadd(List.of("zadd", "20.4", "v2"));
            })
            .compose(v -> {
               ctx.verify(() -> assertThat(v.toLong()).isEqualTo(1));
               return redis.zrange(List.of("zadd", "0", "-1"));
            })
            .compose(v -> {
               ctx.verify(() -> assertThat(v)
                     // [v1, 10.4, v2, 20.4]
                     .hasSize(4)
                     .isInstanceOfSatisfying(MultiType.class, mt -> {
                        assertThat(mt.get(0).toString()).isEqualTo("v1");
                        assertThat(mt.get(1).toDouble()).isEqualTo(10.4);
                        assertThat(mt.get(2).toString()).isEqualTo("v2");
                        assertThat(mt.get(3).toDouble()).isEqualTo(20.4);
                     }));
               return redis.zcount("zadd", "(10.4", "20.4");
            })
            .onSuccess(v -> {
               ctx.verify(() -> assertThat(v.toLong()).isEqualTo(1));
               ctx.completeNow();
            });
   }
}
