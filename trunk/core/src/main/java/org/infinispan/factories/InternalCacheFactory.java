/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2000 - 2008, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.factories;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.CacheDelegate;
import org.infinispan.config.Configuration;
import org.infinispan.config.ConfigurationException;
import org.infinispan.jmx.CacheJmxRegistration;
import org.infinispan.manager.DefaultCacheManager;

/**
 * An internal factory for constructing Caches.  Used by the {@link DefaultCacheManager}, this is not intended as public
 * API.
 * <p/>
 * This is a special instance of a {@link AbstractComponentFactory} which contains bootstrap information for the {@link
 * ComponentRegistry}.
 * <p/>
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 * @since 4.0
 */
public class InternalCacheFactory<K, V> extends AbstractNamedCacheComponentFactory {
   private ClassLoader defaultClassLoader;

   /**
    * This implementation clones the configuration passed in before using it.
    *
    * @param configuration           to use
    * @param globalComponentRegistry global component registry to attach the cache to
    * @param cacheName               name of the cache
    * @return a cache
    * @throws ConfigurationException if there are problems with the cfg
    */
   public Cache<K, V> createCache(Configuration configuration, GlobalComponentRegistry globalComponentRegistry, String cacheName) throws ConfigurationException {
      try {
         return createAndWire(configuration, globalComponentRegistry, cacheName);
      }
      catch (ConfigurationException ce) {
         throw ce;
      }
      catch (RuntimeException re) {
         throw re;
      }
      catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   public Cache<K, V> createDefaultCache(Configuration configuration) throws ConfigurationException {
      return createCache(configuration, null, DefaultCacheManager.DEFAULT_CACHE_NAME);
   }

   protected AdvancedCache<K, V> createAndWire(Configuration configuration, GlobalComponentRegistry globalComponentRegistry, String cacheName) throws Exception {
      AdvancedCache<K, V> spi = new CacheDelegate<K, V>(cacheName);
      bootstrap(cacheName, spi, configuration, globalComponentRegistry);
      return spi;
   }

   /**
    * Bootstraps this factory with a Configuration and a ComponentRegistry.
    */
   private void bootstrap(String cacheName, AdvancedCache spi, Configuration configuration, GlobalComponentRegistry globalComponentRegistry) {
      this.configuration = configuration;

      // injection bootstrap stuff
      componentRegistry = new ComponentRegistry(cacheName, configuration, spi, globalComponentRegistry);
      componentRegistry.registerDefaultClassLoader(defaultClassLoader);
      componentRegistry.registerComponent(spi, AdvancedCache.class);
      componentRegistry.registerComponent(new CacheJmxRegistration(), CacheJmxRegistration.class);
   }

   /**
    * Allows users to specify a default class loader to use for both the construction and running of the cache.
    *
    * @param loader class loader to use as a default.
    */
   public void setDefaultClassLoader(ClassLoader loader) {
      this.defaultClassLoader = loader;
   }

   @Override
   public <T> T construct(Class<T> componentType) {
      throw new UnsupportedOperationException("Should never be invoked - this is a bootstrap factory.");
   }
}
