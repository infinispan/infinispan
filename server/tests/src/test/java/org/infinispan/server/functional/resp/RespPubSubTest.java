package org.infinispan.server.functional.resp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.commons.test.Eventually.eventually;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxTestContext;
import io.vertx.redis.client.Command;
import io.vertx.redis.client.Redis;
import io.vertx.redis.client.RedisConnection;
import io.vertx.redis.client.Request;

public class RespPubSubTest extends AbstractRespTest {

   private static final String CHANNEL = "channel-test";

   @Test
   public void testBasicPubSubOperations(Vertx vertx, VertxTestContext ctx) throws Exception {
      RedisConnection publisher = blockingGetConnection(vertx);
      RedisConnection subscriber = blockingGetConnection(vertx);
      BlockingQueue<Map.Entry<String, String>> queue = subscribe(subscriber, ctx);

      assertThat(queue.poll(10, TimeUnit.SECONDS))
            .isEqualTo(Map.entry("subscribe", "1"));

      List<Future<?>> futures = new ArrayList<>();
      for (int i = 0; i < 10; i++) {
         String content = "content-" + i;
         Future<?> f = publisher.send(Request.cmd(Command.PUBLISH).arg(CHANNEL).arg(content))
               .onFailure(ctx::failNow);

         Map.Entry<String, String> message = queue.poll(10, TimeUnit.SECONDS);
         assertThat(message)
               .satisfies(e -> assertThat(e.getKey()).isEqualTo("message"))
               .satisfies(e -> assertThat(e.getValue()).isEqualTo(content));

         futures.add(f);
      }

      Future<?> f = subscriber.send(Request.cmd(Command.UNSUBSCRIBE).arg(CHANNEL))
            .onFailure(ctx::failNow);
      futures.add(f);

      // We only verify
      Map.Entry<String, String> message = queue.poll(10, TimeUnit.SECONDS);
      assertThat(message)
            .satisfies(e -> assertThat(e.getKey()).isEqualTo("unsubscribe"))
            .satisfies(e -> assertThat(e.getValue()).isEqualTo("0"));

      Future.all(futures)
            .onFailure(ctx::failNow)
            .onComplete(ignore -> ctx.completeNow());
   }

   private BlockingQueue<Map.Entry<String, String>> subscribe(RedisConnection conn, VertxTestContext ctx) {
      BlockingQueue<Map.Entry<String, String>> queue = new LinkedBlockingQueue<>();
      conn.send(Request.cmd(Command.SUBSCRIBE).arg(CHANNEL), ctx.succeeding())
            .handler(msg -> {
               Map.Entry<String, String> entry = Map.entry(msg.get(0).toString(), msg.get(2).toString());
               queue.add(entry);
            });

      return queue;
   }

   private RedisConnection blockingGetConnection(Vertx vertx) {
      Redis subscriber = createBaseClient(vertx);
      Future<RedisConnection> f = subscriber.connect();
      eventually(f::succeeded);

      return f.result();
   }
}
