package org.infinispan.rest.profiling;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.rest.RestCacheClient;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.configuration.RestClientConfiguration;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.test.Eventually;
import org.infinispan.commons.util.Util;

import io.netty.util.CharsetUtil;

/**
 * Benchmarking HTTP/1.1 and HTTP/2 is always comparing apples to bananas. Those protocols are totally different and it
 * doesn't really whether we will use the same or other clients.
 * <p>
 * Unfortunately currently there is no good support for HTTP/2 with TLS/ALPN clients. The only implementation which was
 * reasonably good in testing was Netty (even though a lot of boilerplate code had to be generated). On the other hand
 * HTTP/1.1 is tested using Jetty client. This client unifies the API for both of them.
 */
public class BenchmarkHttpClient {

   private static final RestEntity CACHE_VALUE = RestEntity.create(MediaType.APPLICATION_OCTET_STREAM, "test".getBytes(CharsetUtil.UTF_8));

   private final RestCacheClient cacheClient;

   private final ExecutorCompletionService executorCompletionService;
   private final ExecutorService executor;
   private final RestClient client;

   public BenchmarkHttpClient(RestClientConfiguration configuration, int threads) {
      client = RestClient.forConfiguration(configuration);
      cacheClient = client.cache("default");
      executor = Executors.newFixedThreadPool(threads);
      executorCompletionService = new ExecutorCompletionService(executor);
   }

   public void performGets(int pertentageOfMisses, int numberOfGets, String existingKey, String nonExistingKey) throws Exception {
      Random r = ThreadLocalRandom.current();
      AtomicInteger count = new AtomicInteger();
      for (int i = 0; i < numberOfGets; ++i) {
         String key = r.nextInt(100) < pertentageOfMisses ? nonExistingKey : existingKey;
         executorCompletionService.submit(() -> {
            count.incrementAndGet();
            cacheClient.get(key).whenComplete((resp, e) -> count.decrementAndGet());
            return 1;
         });
      }
      Eventually.eventually(() -> count.get() == 0);
   }

   public void performPuts(int numberOfInserts) {
      AtomicInteger count = new AtomicInteger();
      for (int i = 0; i < numberOfInserts; ++i) {
         String randomKey = Util.threadLocalRandomUUID().toString();
         executorCompletionService.submit(() -> {
            count.incrementAndGet();
            cacheClient.post(randomKey, CACHE_VALUE).whenComplete((response, e) -> count.decrementAndGet());
            return 1;
         });
      }
      Eventually.eventually(() -> count.get() == 0);
   }

   public void stop() throws IOException {
      client.close();
      executor.shutdownNow();
   }

}
