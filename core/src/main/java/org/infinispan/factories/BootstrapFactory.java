package org.infinispan.factories;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.factories.annotations.SurvivesRestarts;

/**
 * Factory for setting up bootstrap components
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 4.0
 */
@DefaultFactoryFor(classes = {Cache.class, AdvancedCache.class, Configuration.class, ComponentRegistry.class})
@SurvivesRestarts
public class BootstrapFactory extends AbstractNamedCacheComponentFactory {
   AdvancedCache<?, ?> advancedCache;

   public BootstrapFactory(AdvancedCache<?, ?> advancedCache, Configuration configuration, ComponentRegistry componentRegistry) {
      this.componentRegistry = componentRegistry;
      this.configuration = configuration;
      this.advancedCache = advancedCache;
   }

   @Override
   public <T> T construct(Class<T> componentType) {
      Object comp = null;
      if (componentType.isAssignableFrom(AdvancedCache.class)) {
         comp = advancedCache;
      } else if (componentType.isAssignableFrom(Configuration.class)) {
         comp = configuration;
      } else if (componentType.isAssignableFrom(ComponentRegistry.class)) {
         comp = componentRegistry;
      }
      if (comp == null) throw new CacheException("Don't know how to handle type " + componentType);

      try {
         return componentType.cast(comp);
      } catch (Exception e) {
         throw new CacheException("Problems casting bootstrap component " + comp.getClass() + " to type " + componentType, e);
      }
   }
}
