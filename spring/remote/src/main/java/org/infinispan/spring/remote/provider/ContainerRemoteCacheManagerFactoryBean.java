package org.infinispan.spring.remote.provider;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.cache.CacheManager;
import org.springframework.util.Assert;

/**
 * {@link FactoryBean} for creating a {@link CacheManager} for a pre-defined {@link RemoteCacheManager}.
  * Useful when the cache container is defined outside the application (e.g. provided by the application server)
 *
 * @author Marius Bogoevici
 */
public class ContainerRemoteCacheManagerFactoryBean implements FactoryBean<CacheManager> {

   private final RemoteCacheManager cacheContainer;

   public ContainerRemoteCacheManagerFactoryBean(RemoteCacheManager cacheContainer) {
      Assert.notNull(cacheContainer, "CacheContainer cannot be null");
      this.cacheContainer = cacheContainer;
   }

   @Override
   public CacheManager getObject() {
      return new SpringRemoteCacheManager(this.cacheContainer);
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
