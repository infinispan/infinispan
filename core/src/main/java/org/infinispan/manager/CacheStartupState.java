package org.infinispan.manager;

/**
 * Tracks the startup state of a cache during container initialization.
 *
 * <p>
 * State transitions:
 * <pre>
 *   STARTING -> READY   (cache started successfully)
 *   STARTING -> FAILED   (cache startup threw an exception, or shutdown occurred)
 * </pre>
 * Terminal states ({@link CacheStartupState#READY} and {@link CacheStartupState#FAILED}) are never revisited.
 *
 * @since 16.2
 * @author José Bolina
 */
public enum CacheStartupState {

   /**
    * The cache has been submitted for startup and initialization is in progress. This includes component wiring, state
    * transfer, and persistence loading. The cache is not ready for use yet.
    */
   STARTING,

   /**
    * The cache started successfully and is available to serve requests.
    */
   READY,

   /**
    * The cache failed to start. The failure is logged and does not affect other caches or the cache manager. The cache
    * can be retried manually via {@link EmbeddedCacheManager#getCache(String)}.
    */
   FAILED
}
