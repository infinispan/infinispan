package org.infinispan.server.test.api;

import java.io.Closeable;
import java.net.InetSocketAddress;

import org.infinispan.server.test.core.TestClient;
import org.infinispan.server.test.core.TestServer;
import org.infinispan.server.test.core.TestSystemPropertyNames;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.redis.client.Redis;
import io.vertx.redis.client.RedisClientType;
import io.vertx.redis.client.RedisConnection;
import io.vertx.redis.client.RedisOptions;

public class RespTestClientDriver extends AbstractTestClientDriver<RespTestClientDriver> {

   private final TestServer testServer;
   private final TestClient testClient;
   private Vertx vertx;
   private RedisOptions options;
   private boolean requireSsl;
   private int port = 11222;

   public RespTestClientDriver(TestServer testServer, TestClient testClient) {
      this.testServer = testServer;
      this.testClient = testClient;
   }

   public RespTestClientDriver withPort(int port) {
      this.port = port;
      return this;
   }

   @Override
   public RespTestClientDriver self() {
      return this;
   }

   public RespTestClientDriver withOptions(RedisOptions options) {
      if (!RedisOptions.DEFAULT_ENDPOINT.equals(options.getEndpoint())) {
         throw new IllegalStateException("Endpoints should not be configured!");
      }
      this.options = options;
      return this;
   }

   public RespTestClientDriver requireSsl(boolean requireSsl) {
      this.requireSsl = requireSsl;
      return this;
   }

   public RespTestClientDriver withVertx(Vertx vertx) {
      this.vertx = vertx;
      return this;
   }

   public Redis get() {
      RespVertxClient client = new RespVertxClient(applyDefaultOptions(), vertx);
      testClient.registerResource(client);
      return client.get();
   }

   public Redis get(int index) {
      RespVertxClient client = new RespVertxClient(specificServerOptions(index), vertx);
      testClient.registerResource(client);
      return client.get();
   }

   public String connectionString() {
      return String.join("", applyDefaultOptions().getEndpoints());
   }

   public String connectionString(int index) {
      return specificServerOptions(index).getEndpoint();
   }

   public RedisConnection connect(Redis client) {
      RedisConnection conn = client.connect().result();
      testClient.registerResource(conn::close);
      return conn;
   }

   private RedisOptions applyUrl(RedisOptions redisOptions, InetSocketAddress serverSocket) {
      StringBuilder sb = new StringBuilder();
      sb.append(requireSsl ? "rediss://" : "redis://");
      if (user != null && user != TestUser.ANONYMOUS) {
         sb.append(user.getUser()).append(":").append(user.getPassword()).append("@");
      } else if (userPerm != null) {
         sb.append(userPerm.name().toLowerCase()).append("_user").append(":").append(userPerm.name().toLowerCase()).append("@");
      }
      sb.append(serverSocket.getHostString()).append(":").append(serverSocket.getPort());
      if (requireSsl) {
         sb.append("?verifyPeer=NONE");
      }
      return redisOptions.addConnectionString(sb.toString());
   }

   protected RedisOptions applyDefaultOptions() {
      int size = testServer.getDriver().getConfiguration().numServers();
      RedisOptions opts = (options != null ? options : new RedisOptions())
            .setPoolName("resp-tests-pool");

      if (size > 1 && !testServer.getDriver().getConfiguration().properties().containsKey(TestSystemPropertyNames.RESP_FORCE_STANDALONE_MODE)) {
         opts = opts.setType(RedisClientType.CLUSTER);
      } else {
         opts = opts.setType(RedisClientType.STANDALONE);
      }
      for (int i = 0; i < size; i++) {
         opts = applyUrl(opts, testServer.getDriver().getServerSocket(i, port));
      }
      return opts;
   }

   protected RedisOptions specificServerOptions(int index) {
      InetSocketAddress serverSocket = testServer.getDriver().getServerSocket(index, port);
      RedisOptions redisOptions = new RedisOptions()
            .setType(RedisClientType.STANDALONE)
            .setMaxPoolWaiting(-1)
            .setPoolName("resp-index-" + index);

      redisOptions = applyUrl(redisOptions, serverSocket);

      return redisOptions;
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
