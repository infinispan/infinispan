package org.infinispan.client.openapi.impl.jdk;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

import org.infinispan.client.openapi.ApiException;
import org.infinispan.client.openapi.api.CacheApi;

public class CacheJDK {
   private final CacheApi cacheApi;
   private final String name;
   private final Executor executor;

   public CacheJDK(String name, CacheApi cacheApi, Executor executor) {
      this.name = name;
      this.cacheApi = cacheApi;
      this.executor = executor;
   }

   /**
    * GETs a key from the cache
    *
    * @param key
    * @return
    */
   public CompletionStage<String> get(String key) {
      return get(key, Collections.emptyMap(), false);
   }

   /**
    * Same as {@link #get(String)} but allowing custom headers.
    */
   public CompletionStage<String> get(String key, Map<String, String> headers) {
      return get(key, headers, false);
   }
   /**
    * GETs a key from the cache with a specific MediaType or list of MediaTypes.
    */
   public CompletionStage<String> get(String key, String mediaType) {
      Map<String, String> headers = mediaType != null ? Map.of("Accept", mediaType) : null;
      return get(key, headers, false);
   }

   /**
    * Same as {@link #get(String, String)} but with an option to return extended headers.
    */
   public CompletionStage<String> get(String key, String mediaType, boolean extended) {
      Map<String, String> headers = mediaType != null ? Map.of("Accept", mediaType) : null;
      return get(key, headers, extended);
   }

   /**
    * Internal get implementation with all parameters
    */
   private CompletionStage<String> get(String key, Map<String, String> headers, boolean extended) {
      return CompletableFuture.supplyAsync(() -> {
         try {
            return cacheApi.getCacheEntry(name, key, extended, headers);
         } catch (ApiException e) {
            throw new CompletionException(e);
         }
      }, executor);
   }
}
