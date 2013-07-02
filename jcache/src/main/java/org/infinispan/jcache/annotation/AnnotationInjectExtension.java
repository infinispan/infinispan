package org.infinispan.jcache.annotation;

import org.infinispan.jcache.annotation.solder.AnnotatedTypeBuilder;

import javax.cache.annotation.CachePut;
import javax.cache.annotation.CacheRemoveAll;
import javax.cache.annotation.CacheRemoveEntry;
import javax.cache.annotation.CacheResult;
import javax.enterprise.event.Observes;
import javax.enterprise.inject.spi.BeforeBeanDiscovery;
import javax.enterprise.inject.spi.Extension;
import javax.enterprise.inject.spi.ProcessAnnotatedType;

/**
 * CDI extension to allow injection of of cache values based on annotations
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
public class AnnotationInjectExtension implements Extension {

   void registerInterceptorBindings(@Observes BeforeBeanDiscovery event) {
      event.addInterceptorBinding(CacheResult.class);
      event.addInterceptorBinding(CachePut.class);
      event.addInterceptorBinding(CacheRemoveEntry.class);
      event.addInterceptorBinding(CacheRemoveAll.class);
   }

   void registerCacheResultInterceptor(@Observes ProcessAnnotatedType<CacheResultInterceptor> event) {
      event.setAnnotatedType(new AnnotatedTypeBuilder<CacheResultInterceptor>()
            .readFromType(event.getAnnotatedType())
            .addToClass(CacheResultLiteral.INSTANCE)
            .create());
   }

   void registerCachePutInterceptor(@Observes ProcessAnnotatedType<CachePutInterceptor> event) {
      event.setAnnotatedType(new AnnotatedTypeBuilder<CachePutInterceptor>()
            .readFromType(event.getAnnotatedType())
            .addToClass(CachePutLiteral.INSTANCE)
            .create());
   }

   void registerCacheRemoveEntryInterceptor(@Observes ProcessAnnotatedType<CacheRemoveEntryInterceptor> event) {
      event.setAnnotatedType(new AnnotatedTypeBuilder<CacheRemoveEntryInterceptor>()
            .readFromType(event.getAnnotatedType())
            .addToClass(CacheRemoveEntryLiteral.INSTANCE)
            .create());
   }

   void registerCacheRemoveAllInterceptor(@Observes ProcessAnnotatedType<CacheRemoveAllInterceptor> event) {
      event.setAnnotatedType(new AnnotatedTypeBuilder<CacheRemoveAllInterceptor>()
            .readFromType(event.getAnnotatedType())
            .addToClass(CacheRemoveAllLiteral.INSTANCE)
            .create());
   }

}
