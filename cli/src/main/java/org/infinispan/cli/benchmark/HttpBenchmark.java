package org.infinispan.cli.benchmark;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.rest.RestCacheClient;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.RestURI;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.util.Util;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.0
 **/
@State(Scope.Thread)
public class HttpBenchmark {
   private static final long BIG_DELAY_NANOS = TimeUnit.DAYS.toNanos(1);
   RestClient client;
   RestCacheClient cache;

   @Param("http://127.0.0.1")
   public String uri;

   @Param("benchmark")
   public String cacheName;

   @Param("")
   public String cacheTemplate;

   @Param("16")
   public int keySize;

   @Param("1000")
   public int valueSize;

   @Param("1000")
   public int keySetSize;

   RestEntity value;
   List<String> keySet;
   AtomicInteger nextIndex;

   @Setup
   public void setup() {
      RestURI uri = RestURI.create(this.uri);
      RestClientConfigurationBuilder builder = uri.toConfigurationBuilder();
      client = RestClient.forConfiguration(builder.build());
      cache = client.cache(cacheName);
      try (RestResponse response = uncheckedAwait(cache.exists())) {
         switch (response.getStatus()) {
            case RestResponse.OK:
            case RestResponse.NO_CONTENT:
               break;
            case RestResponse.NOT_FOUND:
               Util.close(client);
               throw new IllegalArgumentException("Could not find cache '" + cacheName+"'");
            case RestResponse.UNAUTHORIZED:
               Util.close(client);
               throw new SecurityException(response.getBody());
            default:
               Util.close(client);
               throw new RuntimeException(response.getBody());
         }
      }
      value = RestEntity.create(MediaType.APPLICATION_OCTET_STREAM, new byte[valueSize]);
      keySet = new ArrayList<>(keySetSize);
      Random r = new Random(17); // We always use the same seed to make things repeatable
      byte[] keyBytes = new byte[keySize / 2];
      for (int i = 0; i < keySetSize; i++) {
         r.nextBytes(keyBytes);
         String key = Util.toHexString(keyBytes);
         keySet.add(key);
         cache.put(key, value);
      }
      nextIndex = new AtomicInteger();
   }

   @Benchmark
   public void get(Blackhole bh) {
      try(RestResponse response = uncheckedAwait(cache.get(nextKey()))) {
         bh.consume(response.getBody());
      }
   }

   @Benchmark
   public void put() {
      Util.close(uncheckedAwait(cache.put(nextKey(), value)));
   }

   @TearDown
   public void teardown() {
      Util.close(client);
   }

   private String nextKey() {
      return keySet.get(nextIndex.getAndIncrement() % keySetSize);
   }

   public static <T> T uncheckedAwait(CompletionStage<T> future) {
      try {
         return Objects.requireNonNull(future, "Completable Future must be non-null.").toCompletableFuture().get(BIG_DELAY_NANOS, TimeUnit.NANOSECONDS);
      } catch (java.util.concurrent.TimeoutException e) {
         throw new IllegalStateException("This should never happen!", e);
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         throw new CacheException(e);
      } catch (ExecutionException e) {
         throw new CacheException(e);
      }
   }
}
