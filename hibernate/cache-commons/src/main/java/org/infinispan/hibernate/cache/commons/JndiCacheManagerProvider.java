package org.infinispan.hibernate.cache.commons;

import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import org.hibernate.cfg.Environment;
import org.hibernate.engine.jndi.internal.JndiServiceInitiator;
import org.hibernate.engine.jndi.spi.JndiService;
import org.infinispan.hibernate.cache.commons.util.InfinispanMessageLogger;
import org.infinispan.hibernate.cache.spi.EmbeddedCacheManagerProvider;
import org.infinispan.hibernate.cache.spi.InfinispanProperties;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.manager.impl.AbstractDelegatingEmbeddedCacheManager;
import org.kohsuke.MetaInfServices;

@MetaInfServices(EmbeddedCacheManagerProvider.class)
public class JndiCacheManagerProvider implements EmbeddedCacheManagerProvider {
   private static final InfinispanMessageLogger log = InfinispanMessageLogger.Provider.getLog( JndiCacheManagerProvider.class );

   @Override
   public EmbeddedCacheManager getEmbeddedCacheManager(Properties properties) {
      String jndiManagerName = properties.getProperty(InfinispanProperties.CACHE_MANAGER_RESOURCE_PROP);
      if (jndiManagerName != null) {
         EmbeddedCacheManager cacheManager = locateCacheManager(jndiManagerName, properties);
         return new AbstractDelegatingEmbeddedCacheManager(cacheManager) {
            @Override
            public void stop() {
            }

            @Override
            public void close() {
            }
         };
      }
      String factoryClass = properties.getProperty(Environment.CACHE_REGION_FACTORY);
      if (factoryClass == null) {
         // Factory class might not be defined in WF
         return null;
      } else if (factoryClass.endsWith("JndiInfinispanRegionFactory") || factoryClass.equals("infinispan-jndi")) {
         throw log.propertyCacheManagerResourceNotSet();
      }
      return null;
   }

   private EmbeddedCacheManager locateCacheManager(String jndiNamespace, Properties jndiProperties) {

      JndiService jndiService = JndiServiceInitiator.INSTANCE.initiateService(jndiProperties.entrySet()
            .stream().collect(Collectors.toMap(e -> e.getKey().toString(), Map.Entry::getValue)), null);

      return (EmbeddedCacheManager) jndiService.locate(jndiNamespace);
   }
}
