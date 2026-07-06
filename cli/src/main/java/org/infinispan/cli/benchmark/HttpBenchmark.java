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

/**
 * @since 12.0
 **/
public abstract class HttpBenchmark implements BenchmarkTask {
   private static final long BIG_DELAY_NANOS = TimeUnit.DAYS.toNanos(1);

   private final String uri;
   private final String cacheName;
   private final int keySize;
   private final int valueSize;
   private final int keySetSize;

   RestClient client;
   RestCacheClient cache;
   RestEntity value;
   List<String> keySet;
   AtomicInteger nextIndex;

   private HttpBenchmark(String uri, String cacheName, int keySize, int valueSize, int keySetSize) {
      this.uri = uri;
      this.cacheName = cacheName;
      this.keySize = keySize;
      this.valueSize = valueSize;
      this.keySetSize = keySetSize;
   }

   @Override
   public void setup() {
      RestURI restUri = RestURI.create(this.uri);
      RestClientConfigurationBuilder builder = restUri.toConfigurationBuilder();
      client = RestClient.forConfiguration(builder.build());
      cache = client.cache(cacheName);
      try (RestResponse response = uncheckedAwait(cache.exists())) {
         switch (response.status()) {
            case RestResponse.OK:
            case RestResponse.NO_CONTENT:
               break;
            case RestResponse.NOT_FOUND:
               Util.close(client);
               throw new IllegalArgumentException("Could not find cache '" + cacheName + "'");
            case RestResponse.UNAUTHORIZED:
               Util.close(client);
               throw new SecurityException(response.body());
            default:
               Util.close(client);
               throw new RuntimeException(response.body());
         }
      }
      value = RestEntity.create(MediaType.APPLICATION_OCTET_STREAM, new byte[valueSize]);
      keySet = new ArrayList<>(keySetSize);
      Random r = new Random(17);
      byte[] keyBytes = new byte[keySize / 2];
      for (int i = 0; i < keySetSize; i++) {
         r.nextBytes(keyBytes);
         String key = Util.toHexString(keyBytes);
         keySet.add(key);
         Util.close(uncheckedAwait(cache.put(key, value)));
      }
      nextIndex = new AtomicInteger();
   }

   @Override
   public void teardown() {
      Util.close(client);
   }

   String nextKey() {
      return keySet.get(nextIndex.getAndIncrement() % keySetSize);
   }

   static <T> T uncheckedAwait(CompletionStage<T> future) {
      try {
         return Objects.requireNonNull(future, "Completable Future must be non-null.")
               .toCompletableFuture().get(BIG_DELAY_NANOS, TimeUnit.NANOSECONDS);
      } catch (java.util.concurrent.TimeoutException e) {
         throw new IllegalStateException("This should never happen!", e);
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         throw new CacheException(e);
      } catch (ExecutionException e) {
         throw new CacheException(e);
      }
   }

   public static HttpBenchmark get(String uri, String cacheName, int keySize, int valueSize, int keySetSize) {
      return new HttpBenchmark(uri, cacheName, keySize, valueSize, keySetSize) {
         @Override
         public void run() {
            try (RestResponse response = uncheckedAwait(cache.get(nextKey()))) {
               response.body();
            }
         }

         @Override
         public String name() {
            return "HttpBenchmark.get";
         }
      };
   }

   public static HttpBenchmark put(String uri, String cacheName, int keySize, int valueSize, int keySetSize) {
      return new HttpBenchmark(uri, cacheName, keySize, valueSize, keySetSize) {
         @Override
         public void run() {
            Util.close(uncheckedAwait(cache.put(nextKey(), value)));
         }

         @Override
         public String name() {
            return "HttpBenchmark.put";
         }
      };
   }
}
