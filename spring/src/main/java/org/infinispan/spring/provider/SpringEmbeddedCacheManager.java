package org.infinispan.spring.provider;

import org.infinispan.manager.EmbeddedCacheManager;
import org.springframework.cache.CacheManager;
import org.springframework.util.Assert;

import java.util.Collection;

/**
 * <p>
 * A {@link org.springframework.cache.CacheManager <code>CacheManager</code>} implementation that is
 * backed by an {@link org.infinispan.manager.EmbeddedCacheManager
 * <code>Infinispan EmbeddedCacheManager</code>} instance.
 * </p>
 * <p>
 * Note that this <code>CacheManager</code> <strong>does</strong> support adding new
 * {@link org.infinispan.Cache <code>Caches</code>} at runtime, i.e. <code>Caches</code> added
 * programmatically to the backing <code>EmbeddedCacheManager</code> after this
 * <code>CacheManager</code> has been constructed will be seen by this <code>CacheManager</code>.
 * </p>
 *
 * @author <a href="mailto:olaf DOT bergner AT gmx DOT de">Olaf Bergner</a>
 * @author Marius Bogoevici
 *
 */
public class SpringEmbeddedCacheManager implements CacheManager {

   private final EmbeddedCacheManager nativeCacheManager;

   /**
    * @param nativeCacheManager Underlying cache manager
    */
   public SpringEmbeddedCacheManager(final EmbeddedCacheManager nativeCacheManager) {
      Assert.notNull(nativeCacheManager,
                     "A non-null instance of EmbeddedCacheManager needs to be supplied");
      this.nativeCacheManager = nativeCacheManager;
   }

   @Override
   public SpringCache getCache(final String name) {
      return new SpringCache(this.nativeCacheManager.getCache(name));
   }

   @Override
   public Collection<String> getCacheNames() {
      return this.nativeCacheManager.getCacheNames();
   }

   /**
    * Return the {@link org.infinispan.manager.EmbeddedCacheManager
    * <code>org.infinispan.manager.EmbeddedCacheManager</code>} that backs this
    * <code>CacheManager</code>.
    *
    * @return The {@link org.infinispan.manager.EmbeddedCacheManager
    *         <code>org.infinispan.manager.EmbeddedCacheManager</code>} that backs this
    *         <code>CacheManager</code>
    */
   public EmbeddedCacheManager getNativeCacheManager() {
      return this.nativeCacheManager;
   }

   /**
    * Stop the {@link EmbeddedCacheManager <code>EmbeddedCacheManager</code>} this
    * <code>CacheManager</code> delegates to.
    */
   public void stop() {
      this.nativeCacheManager.stop();
   }
}
