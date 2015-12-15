package org.infinispan.jcache.annotation;

import javax.enterprise.event.Observes;
import javax.enterprise.inject.spi.BeforeBeanDiscovery;
import javax.enterprise.inject.spi.Extension;

import org.infinispan.commons.logging.BasicLogFactory;
import org.jboss.logging.BasicLogger;

import java.lang.annotation.Annotation;

import org.kohsuke.MetaInfServices;

/**
 * CDI extension to register additional interceptor bindings
 *
 * @author Galder Zamarreño
 * @author Pete Muir
 * @since 5.3
 */
@MetaInfServices
public class AnnotationInjectExtension implements Extension {

   private static final BasicLogger log = BasicLogFactory.getLog(AnnotationInjectExtension.class);

   @SuppressWarnings("unchecked")
   void registerInterceptorBindings(@Observes BeforeBeanDiscovery event) {
      try {
         event.addInterceptorBinding((Class<Annotation>) Class.forName("javax.cache.annotation.CacheResult"));
         event.addInterceptorBinding((Class<Annotation>) Class.forName("javax.cache.annotation.CachePut"));
         event.addInterceptorBinding((Class<Annotation>) Class.forName("javax.cache.annotation.CacheRemove"));
         event.addInterceptorBinding((Class<Annotation>) Class.forName("javax.cache.annotation.CacheRemoveAll"));
      } catch (ClassNotFoundException ex) {
         log.debug("Cache API not present on class path, class not found: " + ex.getMessage());
      }
   }

}
