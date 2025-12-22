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

   /**
    * PUTs a key/value to the cache as text/plain
    *
    * @param key
    * @param value
    * @return
    */
   public CompletionStage<Void> put(String key, String value) {
      return put(key, null, value, null, null, Collections.emptyMap());
   }

   /**
    * PUT a key/value to the cache with custom media types for keys and values
    */
   public CompletionStage<Void> put(String key, String keyContentType, String value) {
      return put(key, keyContentType, value, null, null, Collections.emptyMap());
   }

   /**
    * Same as {@link #put(String, String, String)} but allowing custom headers.
    */
   public CompletionStage<Void> put(String key, String keyContentType, String value, Map<String, String> headers) {
      return put(key, keyContentType, value, null, null, headers);
   }

   /**
    * PUT an entry with metadata.
    *
    * @param key            The key
    * @param keyContentType The content type of the key
    * @param value          the value
    * @param ttl            The time to live value in seconds
    * @param maxIdle        The max idle value in seconds
    * @return
    */
   public CompletionStage<Void> put(String key, String keyContentType, String value, long ttl, long maxIdle) {
      return put(key, keyContentType, value, (int) ttl, (int) maxIdle, Collections.emptyMap());
   }

   /**
    * PUTs a key/value to the cache as text/plain with the specified expiration
    *
    * @param key
    * @param value
    * @param ttl time to live in seconds
    * @param maxIdle max idle time in seconds
    * @return
    */
   public CompletionStage<Void> put(String key, String value, long ttl, long maxIdle) {
      return put(key, null, value, (int) ttl, (int) maxIdle, Collections.emptyMap());
   }

   /**
    * Same as {@link #put(String, String)} also allowing one or more <code>org.infinispan.context.Flag</code> to be passed.
    */
   public CompletionStage<Void> put(String key, String value, String... flags) {
      // TODO: Flags need to be passed as headers, not yet implemented in generated API
      return put(key, null, value, null, null, Collections.emptyMap());
   }

   /**
    * Internal put implementation with all parameters
    */
   private CompletionStage<Void> put(String key, String keyContentType, String value,
                                      Integer ttl, Integer maxIdle, Map<String, String> headers) {
      return CompletableFuture.supplyAsync(() -> {
         try {
            cacheApi.putCacheEntry(name, key, value, keyContentType, ttl, maxIdle, headers);
            return null;
         } catch (ApiException e) {
            throw new CompletionException(e);
         }
      }, executor);
   }
}
