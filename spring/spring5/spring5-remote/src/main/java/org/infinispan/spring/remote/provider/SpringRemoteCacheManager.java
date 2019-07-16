package org.infinispan.spring.remote.provider;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.spring.common.provider.SpringCache;
import org.springframework.util.Assert;

/**
 * <p>
 * A {@link org.springframework.cache.CacheManager <code>CacheManager</code>} implementation that is
 * backed by an {@link RemoteCacheManager
 * <code>Infinispan RemoteCacheManager</code>} instance.
 * </p>
 *
 * @author <a href="mailto:olaf DOT bergner AT gmx DOT de">Olaf Bergner</a>
 * @author Marius Bogoevici
 *
 */
public class SpringRemoteCacheManager implements org.springframework.cache.CacheManager {

   private final RemoteCacheManager nativeCacheManager;
   private final ConcurrentMap<String, SpringCache> springCaches = new ConcurrentHashMap<>();
   private volatile long readTimeout;
   private volatile long writeTimeout;

   /**
    * @param nativeCacheManager the underlying cache manager
    */
   public SpringRemoteCacheManager(final RemoteCacheManager nativeCacheManager, long readTimeout, long writeTimeout) {
      Assert.notNull(nativeCacheManager,
                     "A non-null instance of EmbeddedCacheManager needs to be supplied");
      this.nativeCacheManager = nativeCacheManager;
      this.readTimeout = readTimeout;
      this.writeTimeout = writeTimeout;
   }

   public SpringRemoteCacheManager(final RemoteCacheManager nativeCacheManager) {
      this(nativeCacheManager, 0, 0);
   }

   /**
    * @see org.springframework.cache.CacheManager#getCache(String)
    */
   @Override
   public SpringCache getCache(final String name) {
      final RemoteCache<Object, Object> nativeCache = this.nativeCacheManager.getCache(name);
      if (nativeCache == null) {
         springCaches.remove(name);
         return null;
      }

      return springCaches.computeIfAbsent(name, n -> new SpringCache(nativeCache, readTimeout, writeTimeout));
   }

   /**
    * @see org.springframework.cache.CacheManager#getCacheNames()
    */
   @Override
   public Collection<String> getCacheNames() {
      return this.nativeCacheManager.getCacheNames();
   }

   /**
    * Return the {@link RemoteCacheManager
    * <code>org.infinispan.client.hotrod.RemoteCacheManager</code>} that backs this
    * <code>SpringRemoteCacheManager</code>.
    *
    * @return The {@link RemoteCacheManager
    *         <code>org.infinispan.client.hotrod.RemoteCacheManager</code>} that backs this
    *         <code>SpringRemoteCacheManager</code>
    */
   public RemoteCacheManager getNativeCacheManager() {
      return this.nativeCacheManager;
   }

   public long getReadTimeout() {
      return this.readTimeout;
   }

   public long getWriteTimeout() {
      return this.writeTimeout;
   }

   public void setReadTimeout(final long readTimeout) {
      this.readTimeout = readTimeout;
   }

   public void setWriteTimeout(final long writeTimeout) {
      this.writeTimeout = writeTimeout;
   }

   /**
    * Start the {@link RemoteCacheManager
    * <code>org.infinispan.client.hotrod.RemoteCacheManager</code>} that backs this
    * <code>SpringRemoteCacheManager</code>.
    */
   public void start() {
      this.nativeCacheManager.start();
   }

   /**
    * Stop the {@link RemoteCacheManager
    * <code>org.infinispan.client.hotrod.RemoteCacheManager</code>} that backs this
    * <code>SpringRemoteCacheManager</code>.
    */
   public void stop() {
      this.nativeCacheManager.stop();
      this.springCaches.clear();
   }
}
