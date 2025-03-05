package org.infinispan.spring.embedded.provider;

import org.infinispan.manager.EmbeddedCacheManager;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.cache.CacheManager;
import org.springframework.util.Assert;

/**
 * {@link FactoryBean} for creating a {@link CacheManager} for a pre-defined {@link org.infinispan.manager.CacheContainer}.
  * Useful when the cache container is defined outside the application (e.g. provided by the application server)
 *
 * @author Marius Bogoevici
 */
public class ContainerEmbeddedCacheManagerFactoryBean implements FactoryBean<CacheManager> {

   private final EmbeddedCacheManager cacheContainer;

   public ContainerEmbeddedCacheManagerFactoryBean(EmbeddedCacheManager cacheContainer) {
      Assert.notNull(cacheContainer, "CacheContainer cannot be null");
      this.cacheContainer = cacheContainer;
   }

   @Override
   public CacheManager getObject() throws Exception {
      return new SpringEmbeddedCacheManager(this.cacheContainer);
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
