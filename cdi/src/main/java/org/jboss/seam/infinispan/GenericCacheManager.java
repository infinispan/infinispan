package org.jboss.seam.infinispan;

import static org.jboss.seam.solder.bean.Beans.getQualifiers;

import javax.enterprise.inject.Instance;
import javax.enterprise.inject.Produces;
import javax.enterprise.inject.spi.AnnotatedMember;
import javax.enterprise.inject.spi.BeanManager;
import javax.inject.Inject;

import org.infinispan.AdvancedCache;
import org.infinispan.manager.CacheContainer;
import org.jboss.seam.infinispan.event.cache.CacheEventBridge;
import org.jboss.seam.solder.bean.Beans;
import org.jboss.seam.solder.bean.generic.ApplyScope;
import org.jboss.seam.solder.bean.generic.Generic;
import org.jboss.seam.solder.bean.generic.GenericConfiguration;

@GenericConfiguration(Infinispan.class)
public class GenericCacheManager<K, V> {

   @Inject
   private CacheContainer defaultCacheContainer;

   @Inject
   @Generic
   private Instance<CacheContainer> cacheContainer;

   @Inject
   @Generic
   private Infinispan infinispan;

   @Inject
   @Generic
   private AnnotatedMember<?> annotatedMember;

   @Inject
   private CacheEventBridge cacheEventBridge;

   private CacheContainer getCacheContainer() {
      if (cacheContainer.isUnsatisfied()) {
         return defaultCacheContainer;
      } else {
         return cacheContainer.get();
      }
   }

   @Produces
   @ApplyScope
   public AdvancedCache<K, V> getAdvancedCache(BeanManager beanManager) {
      String name = infinispan.value();
      AdvancedCache<K, V> cache = getCacheContainer().<K, V> getCache(name)
            .getAdvancedCache();
      cacheEventBridge
            .registerObservers(
                  getQualifiers(beanManager, annotatedMember.getAnnotations()),
                  cache);
      return cache;
   }

}
