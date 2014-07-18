package org.infinispan.spring.provider;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.cache.CacheManager;
import org.springframework.util.Assert;

/**
 * {@link FactoryBean} for creating a {@link CacheManager} for a pre-defined {@link org.infinispan.manager.CacheContainer}.
 * <p/>
 * Useful when the cache container is defined outside the application (e.g. provided by the application server)
 *
 * @author Marius Bogoevici
 */
public class ContainerCacheManagerFactoryBean implements FactoryBean<CacheManager> {

   private CacheContainer cacheContainer;

   public ContainerCacheManagerFactoryBean(CacheContainer cacheContainer) {
      Assert.notNull(cacheContainer, "CacheContainer cannot be null");
      if (!(cacheContainer instanceof EmbeddedCacheManager ||
                  cacheContainer instanceof RemoteCacheManager)) {
         throw new IllegalArgumentException("CacheContainer must be either an EmbeddedCacheManager or a RemoteCacheManager ");
      }
      this.cacheContainer = cacheContainer;
   }

   @Override
   public CacheManager getObject() throws Exception {
      if (this.cacheContainer instanceof EmbeddedCacheManager) {
         return new SpringEmbeddedCacheManager((EmbeddedCacheManager) this.cacheContainer);
      } else if (this.cacheContainer instanceof RemoteCacheManager) {
         return new SpringRemoteCacheManager((RemoteCacheManager) this.cacheContainer);
      } else {
         throw new IllegalArgumentException("CacheContainer must be either an EmbeddedCacheManager or a RemoteCacheManager ");
      }
   }

   @Override
   public Class<?> getObjectType() {
      return CacheManager.class;
   }

   @Override
   public boolean isSingleton() {
      return true;
   }

}
