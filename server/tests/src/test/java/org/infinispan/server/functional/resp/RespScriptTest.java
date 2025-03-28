package org.infinispan.server.functional.resp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.shouldHaveThrown;
import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.v;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.vertx.core.Vertx;
import io.vertx.junit5.VertxTestContext;
import io.vertx.redis.client.RedisAPI;
import io.vertx.redis.client.impl.types.ErrorType;

public class RespScriptTest extends AbstractRespTest {

   @Test
   public void testEval(Vertx vertx, VertxTestContext ctx) {
      RedisAPI redis = createConnection(vertx);
      redis.eval(List.of("""
                  return redis.call('set', KEYS[1], ARGV[1])
                  """, String.valueOf(1), k(), v())).onFailure(ctx::failNow)
            .compose(ignore -> redis.get(k()).onFailure(ctx::failNow))
            .onSuccess(v -> {
               ctx.verify(() -> assertThat(v.toString()).isEqualTo(v()));
               ctx.completeNow();
            });
   }

   @Test
   public void testEvalRo(Vertx vertx, VertxTestContext ctx) {
      RedisAPI redis = createConnection(vertx);
      redis.evalRo(List.of("""
            return redis.call('set', KEYS[1], ARGV[1])
            """, String.valueOf(1), k(), v())).onFailure(t -> {
               assertThat(t).hasMessageContaining("ERR Write commands are not allowed from read-only scripts.");
               ctx.completeNow();
            }
      ).onSuccess(r -> shouldHaveThrown(ErrorType.class));
   }

   @Test
   public void testEvalSha(Vertx vertx, VertxTestContext ctx) {
      RedisAPI redis = createConnection(vertx);
      redis.script(List.of("load", "return redis.call('set', KEYS[1], ARGV[1])"))
            .onFailure(ctx::failNow)
            .onSuccess(r ->
                  redis.evalsha(List.of(r.toString(), String.valueOf(1), k(), v()))
                        .onFailure(ctx::failNow)
                        .compose(ignore ->
                           redis.get(k()).onFailure(ctx::failNow)
            .onSuccess(v -> {
               ctx.verify(() -> assertThat(v.toString()).isEqualTo(v()));
               ctx.completeNow();
            })));
   }
}
