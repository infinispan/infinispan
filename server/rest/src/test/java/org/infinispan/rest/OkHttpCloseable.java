package org.infinispan.rest;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.configuration.RestClientConfiguration;
import org.infinispan.test.TestingUtil;

import okhttp3.ConnectionPool;
import okhttp3.OkHttpClient;
import okhttp3.internal.connection.RealConnectionPool;

public class OkHttpCloseable implements Closeable {
   private final RestClient client;
   private final ExecutorService executorService;

   private OkHttpCloseable(RestClient client) {
      this.client = client;
      this.executorService = okHttpRealConnectionOperate();
   }

   @Override
   public void close() throws IOException {
      client.close();
      if (executorService != null)
         executorService.shutdown();
   }

   private ExecutorService okHttpRealConnectionOperate() {
      OkHttpClient httpClient = TestingUtil.extractField(client, "httpClient");
      ConnectionPool cp = httpClient.connectionPool();
      RealConnectionPool rcp = TestingUtil.extractField(cp, "delegate");

      // The original runnable would block with `wait`.
      TestingUtil.replaceField(RealConnectionPool.class, rcp,"cleanupRunnable", ignore -> (Runnable) () -> { });
      Executor og = TestingUtil.replaceField(RealConnectionPool.class, rcp, "executor", ignore -> runnable -> { });
      if (og instanceof ExecutorService) {
         return (ExecutorService) og;
      }
      return null;
   }

   public RestClient client() {
      return client;
   }

   public static OkHttpCloseable forConfiguration(RestClientConfiguration cfg) {
      return new OkHttpCloseable(RestClient.forConfiguration(cfg));
   }

   public static OkHttpCloseable forClient(RestClient client) {
      return new OkHttpCloseable(client);
   }
}
