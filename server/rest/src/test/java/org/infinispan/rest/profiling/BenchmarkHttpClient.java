package org.infinispan.rest.profiling;

import static io.netty.buffer.Unpooled.wrappedBuffer;
import static io.netty.handler.codec.http.HttpMethod.GET;
import static io.netty.handler.codec.http.HttpMethod.POST;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.rest.configuration.RestClientConfiguration;
import org.infinispan.commons.util.Eventually;
import org.infinispan.rest.client.NettyHttpClient;

import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
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

   private final NettyHttpClient nettyHttpClient;

   private final ExecutorCompletionService executorCompletionService;
   private final ExecutorService executor;

   public BenchmarkHttpClient(RestClientConfiguration configuration, int threads) {
      nettyHttpClient = NettyHttpClient.forConfiguration(configuration);
      executor = Executors.newFixedThreadPool(threads);
      executorCompletionService = new ExecutorCompletionService(executor);
   }

   public void performGets(int pertentageOfMisses, int numberOfGets, String existingKey, String nonExistingKey) throws Exception {
      Random r = ThreadLocalRandom.current();
      AtomicInteger count = new AtomicInteger();
      for (int i = 0; i < numberOfGets; ++i) {
         String key = r.nextInt(100) < pertentageOfMisses ? nonExistingKey : existingKey;
         executorCompletionService.submit(() -> {
            FullHttpRequest getRequest = new DefaultFullHttpRequest(HTTP_1_1, GET, "/rest/v2/caches/default/" + key);
            count.incrementAndGet();
            nettyHttpClient.sendRequest(getRequest).whenComplete((response, e) -> count.decrementAndGet());
            return 1;
         });
      }
      Eventually.eventually(() -> count.get() == 0);
   }

   public void performPuts(int numberOfInserts) {
      AtomicInteger count = new AtomicInteger();
      for (int i = 0; i < numberOfInserts; ++i) {
         String randomKey = UUID.randomUUID().toString();
         executorCompletionService.submit(() -> {
            FullHttpRequest putValueInCacheRequest = new DefaultFullHttpRequest(HTTP_1_1, POST, "/rest/v2/caches/default/" + randomKey,
                  wrappedBuffer("test".getBytes(CharsetUtil.UTF_8)));
            count.incrementAndGet();
            nettyHttpClient.sendRequest(putValueInCacheRequest).whenComplete((response, e) -> count.decrementAndGet());
            return 1;
         });
      }
      Eventually.eventually(() -> count.get() == 0);
   }

   public void stop() {
      nettyHttpClient.stop();
      executor.shutdownNow();
   }

}
