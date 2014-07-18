package org.infinispan.spring.provider;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.springframework.cache.Cache;
import org.springframework.util.Assert;

import java.util.Collection;

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
   public Cache getCache(final String name) {
      return new SpringRemoteCache(this.nativeCacheManager.getCache(name));
   }

   /**
    * <p>
    * As of Infinispan 4.2.0.FINAL <code>org.infinispan.client.hotrod.RemoteCache</code> does
    * <strong>not</strong> support retrieving the set of all cache names from the hotrod server.
    * This restriction may be lifted in the future. Currently, this operation will always throw an
    * <code>UnsupportedOperationException</code>.
    * </p>
    *
    * @see org.springframework.cache.CacheManager#getCacheNames()
    */
   @Override
   public Collection<String> getCacheNames() {
      throw new UnsupportedOperationException(
            "Operation getCacheNames() is currently not supported.");
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
