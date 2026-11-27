package org.infinispan.spring.embedded.provider;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;

import org.infinispan.Cache;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.spring.common.provider.SpringCache;
import org.springframework.cache.CacheManager;
import org.springframework.util.Assert;

/**
 * <p>
 * A {@link CacheManager} implementation that is
 * backed by an {@link EmbeddedCacheManager
 * Infinispan EmbeddedCacheManager} instance.
 * </p>
 * <p>
 * Note that this <code>CacheManager</code> <b>does</b> support adding new
 * {@link Cache Caches} at runtime, i.e. <code>Caches</code> added
 * programmatically to the backing <code>EmbeddedCacheManager</code> after this
 * <code>CacheManager</code> has been constructed will be seen by this <code>CacheManager</code>.
 * </p>
 *
 * @author Olaf Bergner
 * @author Marius Bogoevici
 *
 */
public class SpringEmbeddedCacheManager implements CacheManager {

   private final EmbeddedCacheManager nativeCacheManager;
   private final ConcurrentMap<String, SpringCache> springCaches = new ConcurrentHashMap<>();
   private final boolean reactive;
   private final Predicate<String> putForExternalReadOnLoad;

   /**
    * @param nativeCacheManager Underlying cache manager
    */
   public SpringEmbeddedCacheManager(final EmbeddedCacheManager nativeCacheManager) {
      this(nativeCacheManager, false);
   }

   /**
    * @param nativeCacheManager Underlying cache manager
    */
   public SpringEmbeddedCacheManager(final EmbeddedCacheManager nativeCacheManager, boolean reactive) {
      this(nativeCacheManager, reactive, false);
   }

   /**
    * @param nativeCacheManager       Underlying cache manager
    * @param reactive                 Whether reactive mode is enabled
    * @param putForExternalReadOnLoad Whether values computed by a value loader on a cache miss
    *                                 (e.g. {@code @Cacheable(sync = true)}) are stored with
    *                                 {@link Cache#putForExternalRead(Object, Object)}. In invalidation mode this
    *                                 avoids invalidating the key on the other nodes when a node merely populates
    *                                 its local cache from the source of truth.
    * @see SpringEmbeddedCache
    */
   public SpringEmbeddedCacheManager(final EmbeddedCacheManager nativeCacheManager, boolean reactive,
                                     boolean putForExternalReadOnLoad) {
      this(nativeCacheManager, reactive, cacheName -> putForExternalReadOnLoad);
   }

   /**
    * @param nativeCacheManager       Underlying cache manager
    * @param reactive                 Whether reactive mode is enabled
    * @param putForExternalReadOnLoad Selects, by cache name, the caches for which values computed by a value loader
    *                                 on a cache miss are stored with {@link Cache#putForExternalRead(Object, Object)}
    * @see SpringEmbeddedCache
    */
   public SpringEmbeddedCacheManager(final EmbeddedCacheManager nativeCacheManager, boolean reactive,
                                     Predicate<String> putForExternalReadOnLoad) {
      Assert.notNull(nativeCacheManager,
                     "A non-null instance of EmbeddedCacheManager needs to be supplied");
      Assert.notNull(putForExternalReadOnLoad, "A non-null putForExternalReadOnLoad predicate needs to be supplied");
      this.nativeCacheManager = nativeCacheManager;
      this.reactive = reactive;
      this.putForExternalReadOnLoad = putForExternalReadOnLoad;
   }

   @Override
   public SpringCache getCache(final String name) {
      return springCaches.computeIfAbsent(name, n -> {
         final Cache<Object, Object> nativeCache = this.nativeCacheManager.getCache(n);

         return new SpringEmbeddedCache(nativeCache, reactive, putForExternalReadOnLoad.test(n));
      });
   }

   @Override
   public Collection<String> getCacheNames() {
      return this.nativeCacheManager.getCacheNames();
   }

   /**
    * Return the {@link EmbeddedCacheManager
    * org.infinispan.manager.EmbeddedCacheManager} that backs this
    * <code>CacheManager</code>.
    *
    * @return The {@link EmbeddedCacheManager
    *         org.infinispan.manager.EmbeddedCacheManager} that backs this
    *         <code>CacheManager</code>
    */
   public EmbeddedCacheManager getNativeCacheManager() {
      return this.nativeCacheManager;
   }

   /**
    * Stop the {@link EmbeddedCacheManager} this
    * <code>CacheManager</code> delegates to.
    */
   public void stop() {
      this.nativeCacheManager.stop();
      this.springCaches.clear();
   }
}
