package org.infinispan.hibernate.cache.commons;

import java.util.Properties;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.hibernate.cfg.Environment;
import org.hibernate.internal.util.jndi.JndiHelper;
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
         EmbeddedCacheManager cacheManager = locateCacheManager(jndiManagerName, JndiHelper.extractJndiProperties(properties));
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
      Context ctx = null;
      try {
         ctx = new InitialContext( jndiProperties );
         return (EmbeddedCacheManager) ctx.lookup( jndiNamespace );
      }
      catch (NamingException ne) {
         throw log.unableToRetrieveCmFromJndi(jndiNamespace);
      }
      finally {
         if ( ctx != null ) {
            try {
               ctx.close();
            }
            catch (NamingException ne) {
               log.unableToReleaseContext(ne);
            }
         }
      }
   }
}
