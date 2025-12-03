package org.infinispan.server.resp.metadata;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.infinispan.server.resp.SingleNodeRespBaseTest;
import org.infinispan.server.resp.meta.ClientMetadata;
import org.testng.annotations.Test;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.pubsub.api.sync.RedisPubSubCommands;

@Test(groups = "functional", testName = "server.resp.metadata.InfoClientsTests")
public class InfoClientsTests extends SingleNodeRespBaseTest {

   @Override
   public Object[] factory() {
      return new Object[]{
         new InfoClientsTests(),
         new InfoClientsTests().withAuthorization()
      };
   }

   public void testConnectedClients() throws Throwable {
      ClientMetadata metadata = server.metadataRepository().client();

      // Only a single client is connected.
      RedisCommands<String, String> c0 = redisConnection.sync();
      String info = c0.info("Clients");
      assertThat(metadata.getConnectedClients()).isEqualTo(1);
      assertInfoValue(c0, Map.entry("connected_clients", 1L));

      // The single client start watching 3 keys for TX.
      c0.watch("key1", "key2", "key3");
      assertThat(metadata.getWatchingClients()).isEqualTo(1);
      assertThat(metadata.getWatchedKeys()).isEqualTo(3);
      assertInfoValue(c0, Map.entry("watching_clients", 1L), Map.entry("total_watched_keys", 3L));

      c0.multi();
      c0.set("key1", "value");
      c0.exec();

      // After execution, no watching information.
      assertThat(metadata.getWatchingClients()).isZero();
      assertThat(metadata.getWatchedKeys()).isZero();
      assertInfoValue(c0, Map.entry("watching_clients", 0L), Map.entry("total_watched_keys", 0L));

      // New asynchronous client connect. One more client now.
      // We'll start the asynchronous operations with two clients.
      StatefulRedisConnection<String, String> conn = newConnection();
      RedisAsyncCommands<String, String> c1 = conn.async();
      assertThat(metadata.getConnectedClients()).isEqualTo(2);
      assertInfoValue(c0, Map.entry("connected_clients", 2L));

      // Blocks in keys. Operation is asynchronous, so we keep polling until a client is blocked.
      RedisFuture<?> fut = c1.blpop(0L, "my-list1", "my-list2", "my-list3");
      eventually(() -> metadata.getBlockedClients() == 1);
      assertThat(metadata.getBlockedKeys()).isEqualTo(3);
      assertInfoValue(c0, Map.entry("blocked_clients", 1L), Map.entry("total_blocking_keys", 3L));

      // Synchronous client writes and releases all blocking information.
      c0.lpush("my-list1", "a");
      fut.get(10, TimeUnit.SECONDS);
      assertThat(metadata.getBlockedClients()).isZero();
      assertThat(metadata.getBlockedKeys()).isZero();
      assertInfoValue(c0, Map.entry("blocked_clients", 0L), Map.entry("total_blocking_keys", 0L));

      try (StatefulRedisPubSubConnection<String, String> psconn = client.connectPubSub()) {
         // New connection established.
         assertThat(metadata.getConnectedClients()).isEqualTo(3);
         assertInfoValue(c0, Map.entry("connected_clients", 3L));
         RedisPubSubCommands<String, String> pubSub = psconn.sync();

         // No one is subscribed yet.
         assertThat(metadata.getPubSubClients()).isZero();
         assertInfoValue(c0, Map.entry("pubsub_clients", 0L));

         pubSub.subscribe("channel-1", "channel-2", "channel-3");

         // We have one subscriber per channel.
         assertThat(metadata.getPubSubClients()).isEqualTo(3);
         assertInfoValue(c0, Map.entry("pubsub_clients", 3L));

         pubSub.unsubscribe("channel-1");
         assertThat(metadata.getPubSubClients()).isEqualTo(2);
         assertInfoValue(c0, Map.entry("pubsub_clients", 2L));

         // Unsubscribe all.
         pubSub.unsubscribe();
         assertThat(metadata.getPubSubClients()).isZero();
         assertInfoValue(c0, Map.entry("pubsub_clients", 0L));
      }

      // Netty's event is handled asynchronously.
      eventually(() -> metadata.getConnectedClients() == 2);
      assertInfoValue(c0, Map.entry("connected_clients", 2L));
   }

   @SafeVarargs
   private static void assertInfoValue(RedisCommands<String, String> r, Map.Entry<String, Long> field, Map.Entry<String, Long> ... fields) {
      String info = r.info("Clients");
      assertThat(info).contains(field.getKey() + ":" + field.getValue());

      for (Map.Entry<String, Long> entry : fields) {
         assertThat(info).contains(entry.getKey() + ":" + entry.getValue());
      }
   }
}
