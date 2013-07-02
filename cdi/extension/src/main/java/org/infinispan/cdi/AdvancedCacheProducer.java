package org.infinispan.cdi;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.cdi.event.cache.CacheEventBridge;
import org.infinispan.cdi.event.cachemanager.CacheManagerEventBridge;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.jboss.solder.bean.generic.ApplyScope;
import org.jboss.solder.bean.generic.Generic;
import org.jboss.solder.bean.generic.GenericConfiguration;

import javax.enterprise.inject.Any;
import javax.enterprise.inject.Instance;
import javax.enterprise.inject.Produces;
import javax.enterprise.inject.spi.AnnotatedMember;
import javax.enterprise.inject.spi.BeanManager;
import javax.inject.Inject;

import static org.jboss.solder.bean.Beans.getQualifiers;

/**
 * This class is responsible to produce the {@link Cache} and {@link AdvancedCache}. This class use the
 * <a href="http://docs.jboss.org/seam/3/solder/latest/reference/en-US/html_single/#genericbeans">Generic Beans</a>
 * mechanism provided by Seam Solder.
 *
 * @author Pete Muir
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
@GenericConfiguration(ConfigureCache.class)
public class AdvancedCacheProducer {

   @Inject
   private CacheContainer defaultCacheContainer;

   @Inject
   @Generic
   private Instance<CacheContainer> cacheContainer;

   @Inject
   @Generic
   private ConfigureCache configureCache;

   @Inject
   @Generic
   private AnnotatedMember<?> annotatedMember;

   @Inject
   private CacheEventBridge cacheEventBridge;
   
   @Inject
   private InfinispanExtension infinispanExtension;

   private CacheContainer getCacheContainer() {
      if (cacheContainer.isUnsatisfied()) {
         return defaultCacheContainer;
      } else {
         return cacheContainer.get();
      }
   }

   @Produces
   @ApplyScope
   public <K, V> AdvancedCache<K, V> getAdvancedCache(BeanManager beanManager, CacheManagerEventBridge eventBridge, @Any Instance<EmbeddedCacheManager> cacheManagers) {
      
      // lazy register stuff
      infinispanExtension.registerCacheConfigurations(eventBridge, cacheManagers, beanManager);
       
      final String name = configureCache.value();
      Cache<K, V> cache;

      if (name.isEmpty()) {
         cache = getCacheContainer().getCache();
      } else {
         cache = getCacheContainer().getCache(name);
      }

      cacheEventBridge.registerObservers(
            getQualifiers(beanManager, annotatedMember.getAnnotations()),
            cache
      );

      return cache.getAdvancedCache();
   }
}
