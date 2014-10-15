package org.infinispan.cdi;

import java.lang.annotation.Annotation;
import java.util.Set;

import javax.enterprise.inject.Any;
import javax.enterprise.inject.Instance;
import javax.enterprise.inject.Produces;
import javax.enterprise.inject.spi.BeanManager;
import javax.inject.Inject;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.cdi.event.cache.CacheEventBridge;
import org.infinispan.cdi.event.cachemanager.CacheManagerEventBridge;
import org.infinispan.cdi.util.Reflections;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;

/**
 * This class is responsible to produce the {@link Cache} and {@link AdvancedCache}. This class use the
 * <a href="http://docs.jboss.org/seam/3/solder/latest/reference/en-US/html_single/#genericbeans">Generic Beans</a>
 * mechanism provided by Seam Solder.
 *
 * @author Pete Muir
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
public class AdvancedCacheProducer {

   @Inject
   private CacheContainer defaultCacheContainer;

   @Inject
   private CacheEventBridge cacheEventBridge;

   @Inject
   private InfinispanExtension infinispanExtension;

   @Inject @Any
   private Instance<EmbeddedCacheManager> cacheManagers;

   @Inject
   private BeanManager beanManager;

   @Inject
   private CacheManagerEventBridge eventBridge;

   private CacheContainer getCacheContainer(Set<Annotation> qualifiers) {
      Instance<EmbeddedCacheManager> cacheContainer = cacheManagers.select(qualifiers.toArray(Reflections.EMPTY_ANNOTATION_ARRAY));
      if (cacheContainer.isUnsatisfied()) {
         return defaultCacheContainer;
      } else {
         return cacheContainer.get();
      }
   }

   public <K, V> AdvancedCache<K, V> getAdvancedCache(String name, Set<Annotation> qualifiers) {

      // lazy register stuff
      infinispanExtension.getEmbeddedExtension().registerCacheConfigurations(eventBridge, cacheManagers, beanManager);

      Cache<K, V> cache;
      CacheContainer container = getCacheContainer(qualifiers);
      if (name.isEmpty()) {
         cache = container.getCache();
      } else {
         cache = container.getCache(name);
      }

      cacheEventBridge.registerObservers(
            qualifiers,
            cache
      );

      return cache.getAdvancedCache();
   }

   @Produces
   <K, V> AdvancedCache<K, V> getDefaultAdvancedCache() {
       // lazy register stuff
       infinispanExtension.getEmbeddedExtension().registerCacheConfigurations(eventBridge, cacheManagers, beanManager);
       return defaultCacheContainer.<K, V>getCache().getAdvancedCache();
   }
}
