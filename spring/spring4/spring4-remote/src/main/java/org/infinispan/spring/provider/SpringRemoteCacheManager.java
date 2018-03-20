package org.infinispan.spring.provider;

import java.util.Collection;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.springframework.util.Assert;

/**
 * <p>
 * A {@link org.springframework.cache.CacheManager <code>CacheManager</code>} implementation that is
 * backed by an {@link org.infinispan.client.hotrod.RemoteCacheManager
 * <code>Infinispan RemoteCacheManager</code>} instance.
 * </p>
 *
 * @author <a href="mailto:olaf DOT bergner AT gmx DOT de">Olaf Bergner</a>
 * @author Marius Bogoevici
 *
 */
public class SpringRemoteCacheManager implements org.springframework.cache.CacheManager {

   private final RemoteCacheManager nativeCacheManager;

   /**
    * @param nativeCacheManager the underlying cache manager
    */
   public SpringRemoteCacheManager(final RemoteCacheManager nativeCacheManager) {
      Assert.notNull(nativeCacheManager,
                     "A non-null instance of EmbeddedCacheManager needs to be supplied");
      this.nativeCacheManager = nativeCacheManager;
   }

   /**
    * @see org.springframework.cache.CacheManager#getCache(java.lang.String)
    */
   @Override
   public SpringCache getCache(final String name) {
      return new SpringCache(this.nativeCacheManager.getCache(name));
   }

   /**
    * @see org.springframework.cache.CacheManager#getCacheNames()
    */
   @Override
   public Collection<String> getCacheNames() {
      return this.nativeCacheManager.getCacheNames();
   }

   /**
    * Return the {@link org.infinispan.client.hotrod.RemoteCacheManager
    * <code>org.infinispan.client.hotrod.RemoteCacheManager</code>} that backs this
    * <code>SpringRemoteCacheManager</code>.
    *
    * @return The {@link org.infinispan.client.hotrod.RemoteCacheManager
    *         <code>org.infinispan.client.hotrod.RemoteCacheManager</code>} that backs this
    *         <code>SpringRemoteCacheManager</code>
    */
   public RemoteCacheManager getNativeCacheManager() {
      return this.nativeCacheManager;
   }

   /**
    * Start the {@link org.infinispan.client.hotrod.RemoteCacheManager
    * <code>org.infinispan.client.hotrod.RemoteCacheManager</code>} that backs this
    * <code>SpringRemoteCacheManager</code>.
    */
   public void start() {
      this.nativeCacheManager.start();
   }

   /**
    * Stop the {@link org.infinispan.client.hotrod.RemoteCacheManager
    * <code>org.infinispan.client.hotrod.RemoteCacheManager</code>} that backs this
    * <code>SpringRemoteCacheManager</code>.
    */
   public void stop() {
      this.nativeCacheManager.stop();
   }
}
