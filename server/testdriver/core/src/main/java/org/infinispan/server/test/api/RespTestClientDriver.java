package org.infinispan.server.test.api;

import java.io.Closeable;

import org.infinispan.server.test.core.TestClient;
import org.infinispan.server.test.core.TestServer;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.redis.client.Redis;
import io.vertx.redis.client.RedisConnection;
import io.vertx.redis.client.RedisOptions;

public class RespTestClientDriver extends BaseTestClientDriver<RespTestClientDriver> {

   private final TestServer testServer;
   private final TestClient testClient;
   private Vertx vertx;
   private RedisOptions options;

   public RespTestClientDriver(TestServer testServer, TestClient testClient) {
      this.testServer = testServer;
      this.testClient = testClient;
   }

   @Override
   public RespTestClientDriver self() {
      return this;
   }

   public RespTestClientDriver withOptions(RedisOptions options) {
      this.options = options;
      return this;
   }

   public RespTestClientDriver withVertx(Vertx vertx) {
      this.vertx = vertx;
      return this;
   }

   public Redis get() {
      RespVertxClient client = new RespVertxClient(options, vertx);
      testClient.registerResource(client);
      return client.get();
   }

   public RedisConnection connect(Redis client) {
      RedisConnection conn = client.connect().result();
      testClient.registerResource(conn::close);
      return conn;
   }

   public RedisConnection getConnection() {
      return connect(get());
   }

   private static class RespVertxClient extends AbstractVerticle implements Closeable {

      private final RedisOptions options;
      private Redis client;

      private RespVertxClient(RedisOptions options, Vertx vertx) {
         this.options = options;
         this.vertx = vertx;
      }

      private Redis get() {
         assert client == null : "Vertx Redis client already created";
         return client = Redis.createClient(vertx, options);
      }

      @Override
      public void close() {
         if (client != null) {
            client.close();
         }
      }
   }
}
