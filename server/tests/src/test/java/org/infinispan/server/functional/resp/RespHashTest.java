package org.infinispan.server.functional.resp;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import io.vertx.core.Vertx;
import io.vertx.junit5.VertxTestContext;
import io.vertx.redis.client.RedisAPI;
import io.vertx.redis.client.impl.types.ErrorType;

public class RespHashTest extends AbstractRespTest {

   @Test
   public void testHashOperations(Vertx vertx, VertxTestContext ctx) {
      RedisAPI redis = createConnection(vertx);

      Map<String, String> map = Map.of("key1", "value1", "key2", "value2", "key3", "value3");
      List<String> args = new ArrayList<>();
      args.add("HMSET");
      args.addAll(map.entrySet().stream().flatMap(e -> Stream.of(e.getKey(), e.getValue())).collect(Collectors.toList()));

      redis.hmset(args)
            .onFailure(ctx::failNow)
            .compose(v -> {
               ctx.verify(() -> assertThat(v.toString()).isEqualTo("OK"));
               return redis.hget("HMSET", "key1");
            })
            .compose(v -> {
               ctx.verify(() -> assertThat(v.toString()).isEqualTo("value1"));
               return redis.hget("HMSET", "unknown");
            })
            .onSuccess(v -> ctx.verify(() -> assertThat(v).isNull()))
            .andThen(ignore -> redis.set(List.of("plain", "string")))
            .compose(ignore -> ctx.assertFailure(redis.hmset(List.of("plain", "k1", "v1")))
                  .onFailure(t -> ctx.verify(() -> assertThat(t)
                        .isInstanceOf(ErrorType.class)
                        .hasMessageContaining("ERRWRONGTYPE"))))
            .onComplete(ignore -> ctx.completeNow());
   }
}
